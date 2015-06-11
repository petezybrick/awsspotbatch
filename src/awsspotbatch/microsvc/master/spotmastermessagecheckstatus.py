# Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Process the SpotMasterMessageCheckStatus message
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import json
import uuid
import traceback
import awsext.ec2
import awsext.iam
import awsext.vpc
import boto.dynamodb2
from awsspotbatch.microsvc import get_batch_job_parm_item
from awsspotbatch.microsvc.master import find_spot_request_instance_ids_by_master_uuid, spot_master_row_partial_save, submit_request_spot_instances, fmt_master_uuid_msg_hdr, fmt_master_item_msg_hdr
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import TableSpotMaster, SpotMasterStateCode, TableSpotBatchJobParm
from awsspotbatch.common.batchjobparmitem import deserialize_inbound_rule_items, BatchJobParmItem
from awsspotbatch.common.util import create_policy
from awsspotbatch.microsvc.master.spotmastermessagebase import SpotMasterMessageBase
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.util import create_microsvc_message_attributes



import logging
logger = logging.getLogger(__name__)


class SpotMasterMessageCheckStatus(SpotMasterMessageBase):
    """Process the SpotMasterMessageCheckStatus message"""

    def __init__( self,
                  spot_master_table_name=awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, 
                  spot_master_queue_name=awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, 
                  spot_request_table_name=awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, 
                  spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, 
                  spot_rsa_key_table_name=awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, 
                  spot_batch_job_parm_table_name=awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME,
                  region_name='us-east-1', profile_name=None ):
        """

        :param spot_master_table_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME)
        :param spot_master_queue_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME)
        :param spot_request_table_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME)
        :param spot_request_queue_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME)
        :param spot_rsa_key_table_name:  (Default value = awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME)
        :param spot_batch_job_parm_table_name:  (Default value = awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME)
        :param region_name:  (Default value = 'us-east-1')
        :param profile_name: profile name from the credentials file (Default value = None)

        """

        SpotMasterMessageBase.__init__(self, 
                                  spot_master_table_name=spot_master_table_name, 
                                  spot_master_queue_name=spot_master_queue_name, 
                                  spot_request_table_name=spot_request_table_name, 
                                  spot_request_queue_name=spot_request_queue_name, 
                                  spot_rsa_key_table_name=spot_rsa_key_table_name, 
                                  spot_batch_job_parm_table_name=spot_batch_job_parm_table_name,
                                  region_name=region_name, profile_name=profile_name )

        
    def process( self, message ) :
        """ Process the message

        :param message: SQS Message instance

        """
        try: 
            spot_master_msg = SpotMasterMsg( raw_json=message.get_body() )
            spot_master_uuid = spot_master_msg.spot_master_uuid
            logger.info( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'process_check_status' )
            # Get master row from DynamoDB and process based on state
            dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
            spot_master_table = Table( self.spot_master_table_name, connection=dynamodb_conn ) 
            spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )
            logger.info( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'master state=' + spot_master_item[TableSpotMaster.spot_master_state_code])
            
            next_status_msg_delay_secs = 60
            is_send_master_msg_check_status = True
            master_state_code = spot_master_item[TableSpotMaster.spot_master_state_code]
            spot_master_item[ TableSpotMaster.ts_last_state_check ] = int( time.time() )
            spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, 
                                  {TableSpotMaster.ts_last_state_check:int( time.time() )},
                                  region_name=self.region_name, profile_name=self.profile_name )
            
            # Process based on the current Master State
            if SpotMasterStateCode.master_resources_in_progress == master_state_code:
                self.handle_state_master_resources_in_progress( spot_master_item )
                next_status_msg_delay_secs = 5
            elif SpotMasterStateCode.master_role_policy_in_progress == master_state_code:
                self.handle_state_master_role_policy_in_progress( spot_master_item, dynamodb_conn )
                next_status_msg_delay_secs = 5
            elif SpotMasterStateCode.waiting_for_instances_complete == master_state_code:
                self.handle_state_waiting_for_instances_complete( spot_master_item )
            elif SpotMasterStateCode.waiting_for_instances_terminated == master_state_code:
                self.handle_state_waiting_for_instances_terminated( spot_master_item )
            elif SpotMasterStateCode.waiting_for_master_resources_terminated == master_state_code:
                self.handle_state_waiting_for_master_resources_terminated( spot_master_item )
                next_status_msg_delay_secs = 5
            elif SpotMasterStateCode.cleanup_in_progress == master_state_code:
                self.handle_state_cleanup_in_progress( spot_master_item )
            elif SpotMasterStateCode.cleanup_complete == master_state_code:
                self.handle_state_cleanup_complete( spot_master_item )
                is_send_master_msg_check_status = False
            
            self.spot_master_sqs_message_durable.delete_message(message)        
            
            if is_send_master_msg_check_status:
                spot_master_msg_check_status = SpotMasterMsg( spot_master_uuid=spot_master_uuid, spot_master_msg_type=SpotMasterMsg.TYPE_CHECK_STATUS )
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageCheckStatus )
                self.spot_master_sqs_message_durable.send_message( spot_master_msg_check_status.to_json(), 
                                                              delay_seconds=next_status_msg_delay_secs,
                                                              message_attributes=message_attributes )
        except StandardError as e:
            logger.error( fmt_master_uuid_msg_hdr( spot_master_uuid ) + str(e) )
            logger.error( fmt_master_uuid_msg_hdr( spot_master_uuid ) + traceback.format_exc() )

     
    def handle_state_master_resources_in_progress(self, spot_master_item ):
        """ Verify the SG, KP and Role/InstanceProfile are created

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_resources_in_progress')
        kp_name = spot_master_item[ TableSpotMaster.kp_name ]
        ec2_conn = awsext.ec2.connect_to_region( self.region_name, profile_name=self.profile_name )
        key_pair = ec2_conn.find_key_pair( kp_name )
        if key_pair == None: return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_resources_in_progress: kp_name ready')
        
        vpc_id = spot_master_item[ TableSpotMaster.cheapest_vpc_id ]
        sg_id = spot_master_item[ TableSpotMaster.sg_id ]
        vpc_conn = awsext.vpc.connect_to_region( self.region_name, profile_name=self.profile_name )
        group_id, is_group_exists = vpc_conn.is_security_group_exists( vpc_id, group_id=sg_id )
        if not is_group_exists: return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_resources_in_progress: sg_id ready')
        
        iam_conn = awsext.iam.connect_to_region( self.region_name, profile_name=self.profile_name )
        role_name = spot_master_item[ TableSpotMaster.role_name ]
        if not iam_conn.is_role_exists( role_name ): return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_resources_in_progress: role_name ready')
        if not iam_conn.is_instance_profile_exists( role_name ): return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_resources_in_progress: instance_profile_name ready')
        
        batch_job_parm = get_batch_job_parm_item( spot_master_item[ TableSpotMaster.spot_master_uuid ], 
                                                  self.spot_batch_job_parm_table_name, self.region_name, self.profile_name,
                                                  attributes=[TableSpotBatchJobParm.raw_batch_job_parm_item] )
        raw_batch_job_parm_item = batch_job_parm[ TableSpotBatchJobParm.raw_batch_job_parm_item ]
        batch_job_parm_item = BatchJobParmItem( stringParmFile=raw_batch_job_parm_item )
        # At this point, all resources have been created - some require additional steps after creation completes
        # Update the SG with any inbound rules
        inbound_rule_items_serialized = batch_job_parm_item.serialized_inbound_rule_items
        if inbound_rule_items_serialized != None:
            inbound_rule_items = deserialize_inbound_rule_items( inbound_rule_items_serialized )
            security_group = vpc_conn.get_security_group( vpc_id, group_id )
            vpc_conn.authorize_inbound_rules( security_group, inbound_rule_items )
        
        # Create base policy (queue, buckets) and extend with user policy from batch_job_parm_item
        policy = create_policy( batch_job_parm_item )
        policy_json = json.dumps( policy )
        iam_conn.add_role_instance_profile_policy( role_name=spot_master_item[ TableSpotMaster.role_name ], 
                                                   policy_name=spot_master_item[ TableSpotMaster.policy_name ], 
                                                   policy=policy_json
                                                  )
        spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, 
                                      {TableSpotMaster.spot_master_state_code:SpotMasterStateCode.master_role_policy_in_progress},
                                      region_name=self.region_name, profile_name=self.profile_name )

        return  
        
    
    # Verify the Policy has been assigned to the Role
    def handle_state_master_role_policy_in_progress(self, spot_master_item, dynamodb_conn ):
        """ Verify the Policy is added to the Role 

        :param spot_master_item: 
        :param dynamodb_conn: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_master_role_policy_in_progress')
        iam_conn = awsext.iam.connect_to_region( self.region_name, profile_name=self.profile_name )
        is_role_policy_added = iam_conn.is_role_policy_added( role_name=spot_master_item[ TableSpotMaster.role_name ], 
                                       policy_name=spot_master_item[ TableSpotMaster.policy_name ])
        if not is_role_policy_added: return
        # For some bizarre timing reason, is_role_policy_added can return True but the spot request fails on IAM role not attached to instance profile
        #  - give it a few seconds to clear
        time.sleep(5)
    
        spot_master_state_code = SpotMasterStateCode.waiting_for_instances_complete
        
        # Request spot instances
        spot_instance_requests = submit_request_spot_instances( spot_master_item, self.region_name, self.profile_name )
        
        # Queue up a SpotRequestMsg for each spot request - this will manage all states for SpotRequest        
        if spot_instance_requests != None:
            spot_request_sqs_message_durable = SqsMessageDurable( self.spot_request_queue_name, self.region_name, profile_name=self.profile_name )
            for spot_instance_request in spot_instance_requests:
                spot_request_uuid = str(uuid.uuid1())
                spot_request_msg = SpotRequestMsg( spot_request_uuid=spot_request_uuid, 
                                                   spot_master_uuid=spot_master_item[ TableSpotMaster.spot_master_uuid ], 
                                                   spot_request_msg_type=SpotRequestMsg.TYPE_SPOT_REQUEST_INITIATED, 
                                                   spot_request_id=spot_instance_request.id )
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_SPOT_PRICE ] = str( spot_master_item[TableSpotMaster.cheapest_price])
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_INSTANCE_USERNAME ] = spot_master_item[ TableSpotMaster.instance_username ]
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_ATTEMPT_NUMBER ] = 1
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageSpotRequestInitiated )
                spot_request_sqs_message_durable.send_message( spot_request_msg.to_json(), message_attributes=message_attributes  )
        else: spot_master_state_code = SpotMasterStateCode.no_instances_available
        
        spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {
                                                              TableSpotMaster.spot_master_state_code:spot_master_state_code
                                                              },
                                      region_name=self.region_name, profile_name=self.profile_name  )
                   
    
    def handle_state_waiting_for_instances_complete(self, spot_master_item ):
        """ Check if number of instances == number of successful instances, 
            if so then update Master to state waiting_for_instances_terminated since the 
            requested spot instances are being terminated

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_instances_complete')
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) +  'num instances=' + str( spot_master_item[ TableSpotMaster.num_instances ]) + 
                     ', num complete=' + str( spot_master_item[ TableSpotMaster.num_requests_complete_ok ]) )
        if spot_master_item[ TableSpotMaster.num_instances ] == spot_master_item[ TableSpotMaster.num_requests_complete_ok ]:
            # Initiate termination
            spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {TableSpotMaster.spot_master_state_code:SpotMasterStateCode.waiting_for_instances_terminated},
                                      region_name=self.region_name, profile_name=self.profile_name  )
     
    
    def handle_state_waiting_for_instances_terminated(self, spot_master_item ):
        """ If all instances terminated, clean up - delete role and policy 

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_instances_terminated')
        spot_request_instance_ids = find_spot_request_instance_ids_by_master_uuid( spot_master_item[ TableSpotMaster.spot_master_uuid ],
                                                                                   region_name=self.region_name, profile_name=self.profile_name)
        ec2_conn = awsext.ec2.connect_to_region( self.region_name, profile_name=self.profile_name )
        num_spot_instances = len( spot_request_instance_ids )
        cnt_terminated = 0
        for spot_request_instance_id in spot_request_instance_ids:
            state_name, status = ec2_conn.get_instance_state_name_and_status( spot_request_instance_id )
            if state_name == 'terminated' or state_name == 'not-found': cnt_terminated += 1
        
        # Check if all are terminated
        if num_spot_instances == cnt_terminated:
            logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'All spot instances terminated, begin deleting KeyPair, SecurityGroup and Role')
            # Terminate the master resources - sg, kp, role
            ec2_conn.delete_key_pair( spot_master_item[ TableSpotMaster.kp_name ] )
            vpc_conn = awsext.vpc.connect_to_region( self.region_name, profile_name=self.profile_name )
            vpc_conn.delete_security_group( group_id=spot_master_item[ TableSpotMaster.sg_id ] )           
            iam_conn = awsext.iam.connect_to_region( self.region_name, profile_name=self.profile_name )
            role_name = spot_master_item[ TableSpotMaster.role_name ]
            policy_name = spot_master_item[ TableSpotMaster.policy_name ]
            iam_conn.remove_role_from_instance_profile( role_name, role_name)
            iam_conn.delete_role_policy( role_name, policy_name)  
            iam_conn.delete_role( role_name )
            spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {TableSpotMaster.spot_master_state_code:SpotMasterStateCode.waiting_for_master_resources_terminated},
                                      region_name=self.region_name, profile_name=self.profile_name )
        else: 
            logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'Spot Instance Termination In Progress: ' + str(cnt_terminated) + ' of ' + str(num_spot_instances) + ' are terminated')

    
    def handle_state_waiting_for_master_resources_terminated(self, spot_master_item ):
        """ Verify master resources have been deleted

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_master_resources_terminated')
        kp_name = spot_master_item[ TableSpotMaster.kp_name ]
        ec2_conn = awsext.ec2.connect_to_region( self.region_name, profile_name=self.profile_name )
        key_pair = ec2_conn.find_key_pair( kp_name )
        if key_pair != None: return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_master_resources_terminated: KeyPair deleted')
        
        vpc_id = spot_master_item[ TableSpotMaster.cheapest_vpc_id ]
        sg_id = spot_master_item[ TableSpotMaster.sg_id ]
        vpc_conn = awsext.vpc.connect_to_region( self.region_name, profile_name=self.profile_name )
        group_id, is_group_exists = vpc_conn.is_security_group_exists( vpc_id, group_id=sg_id )
        if is_group_exists: return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_master_resources_terminated: SecurityGroup deleted')
        
        iam_conn = awsext.iam.connect_to_region( self.region_name, profile_name=self.profile_name )
        role_name = spot_master_item[ TableSpotMaster.role_name ]
        if iam_conn.is_role_exists( role_name ): return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_master_resources_terminated: Role deleted')
        if iam_conn.is_instance_profile_exists( role_name ):
            iam_conn.delete_instance_profile( role_name )
            return
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_waiting_for_master_resources_terminated: InstanceProfile deleted')
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'All master resources deleted')
            
        spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {TableSpotMaster.spot_master_state_code:SpotMasterStateCode.cleanup_in_progress},
                                      region_name=self.region_name, profile_name=self.profile_name  )
    
    
    def handle_state_cleanup_in_progress(self, spot_master_item ):
        """ Cleanup any additional resources 

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_cleanup_in_progress')
        # Reserved for future use 
        spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {TableSpotMaster.spot_master_state_code:SpotMasterStateCode.cleanup_complete},
                                      region_name=self.region_name, profile_name=self.profile_name )
    
    
    def handle_state_cleanup_complete(self, spot_master_item ):
        """ Master is totally complete, spot batch job is done

        :param spot_master_item: 

        """
        logger.info( fmt_master_item_msg_hdr( spot_master_item ) + 'handle_state_cleanup_complete')
        spot_master_row_partial_save( self.spot_master_table_name, spot_master_item, {TableSpotMaster.is_open:0, TableSpotMaster.ts_end:int(time.time())},
                                      region_name=self.region_name, profile_name=self.profile_name )
