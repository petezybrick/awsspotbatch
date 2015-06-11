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
Process the SpotMasterMessageSubmitBatch message
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import traceback
import boto.dynamodb2
import awsext.vpc
import awsext.ec2
import awsext.iam
import awsspotbatch.common.util
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg
from awsspotbatch.common.tabledef import TableSpotMaster, SpotMasterStateCode
from awsspotbatch.microsvc.master.spotmastermessagebase import SpotMasterMessageBase
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem
from awsspotbatch.microsvc.master import find_cheapest_subnet_price, put_rsa_key_item, fmt_master_item_msg_hdr, fmt_master_uuid_msg_hdr
from awsspotbatch.microsvc import put_batch_job_parm_item
from awsspotbatch.common.util import create_microsvc_message_attributes



import logging
logger = logging.getLogger(__name__)


class SpotMasterMessageSubmitBatch(SpotMasterMessageBase):
    """ Process the SpotMasterMessageSubmitBatch message"""

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
        :param profile_name:  (Default value = None)

        """

        SpotMasterMessageBase.__init__(self, spot_master_table_name=spot_master_table_name, 
                  spot_master_queue_name=spot_master_queue_name, 
                  spot_request_table_name=spot_request_table_name, 
                  spot_request_queue_name=spot_request_queue_name, 
                  spot_rsa_key_table_name=spot_rsa_key_table_name, 
                  spot_batch_job_parm_table_name=spot_batch_job_parm_table_name,
                  region_name=region_name, profile_name=profile_name )

        
    def process( self, message ) :
        """ User has requested Spot Batch instances - this is the initial entry point for a user request

        :param message: SQS Message instance

        """
        try: 
            spot_master_msg = SpotMasterMsg( raw_json=message.get_body() )
            spot_master_uuid = spot_master_msg.spot_master_uuid
            logger.info( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'process_submit_batch')
            dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
            vpc_conn = awsext.vpc.connect_to_region( self.region_name, profile_name=self.profile_name )
            ec2_conn = awsext.ec2.connect_to_region( self.region_name, profile_name=self.profile_name )
            iam_conn = awsext.iam.connect_to_region( self.region_name, profile_name=self.profile_name )
          
            batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_master_msg.raw_batch_job_parm_item )        
            cheapest_subnet_id, cheapest_price, spot_cheapest_item = find_cheapest_subnet_price( batch_job_parm_item,profile_name=self.profile_name )
            
            if cheapest_subnet_id != None:
                logger.info( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'Starting spot batch job' )
                put_batch_job_parm_item( spot_master_uuid, self.spot_batch_job_parm_table_name, spot_master_msg,
                                         region_name=self.region_name, profile_name=self.profile_name )
                spot_master_state_code = SpotMasterStateCode.master_resources_in_progress
                subnet = vpc_conn.get_all_subnets( subnet_ids=[cheapest_subnet_id] )[0]
                cheapest_vpc_id = subnet.vpc_id
                cheapest_subnet_id = cheapest_subnet_id
                cheapest_region_name = spot_cheapest_item.region.name
                cheapest_zone_name = spot_cheapest_item.zone.name
                unique_key_pair = ec2_conn.create_unique_key_pair( 'spotkp_' )                
                # Store the key for later use in SSH
                rsa_key_encoded = awsspotbatch.common.util.encode( awsspotbatch.common.util.kp_enc_key, unique_key_pair.material )
                put_rsa_key_item( spot_master_uuid, self.spot_rsa_key_table_name, rsa_key_encoded, 
                                  region_name=self.region_name, profile_name=self.profile_name )           
                
                unique_security_group = vpc_conn.create_unique_security_group( cheapest_vpc_id, 'spotsg_' )
                policy = batch_job_parm_item.policy_statements
                security_group_inbound_rule_items_serialized = batch_job_parm_item.serialized_inbound_rule_items
        
                role_instance_profile_item = iam_conn.create_unique_role_instance_profile( policy=policy, 
                                              role_name_prefix=awsspotbatch.common.const.ROLE_NAME_PREFIX,
                                              policy_name_prefix=awsspotbatch.common.const.POLICY_NAME_PREFIX )
                
                #  instance_profile_name, role_name, policy_name
                self.create_master_row( dynamodb_conn, batch_job_parm_item, spot_master_msg=spot_master_msg, spot_master_uuid=spot_master_uuid, 
                                        cheapest_vpc_id=cheapest_vpc_id,
                                        cheapest_subnet_id=cheapest_subnet_id,
                                        cheapest_region_name=cheapest_region_name,
                                        cheapest_zone_name=cheapest_zone_name,
                                        cheapest_price=cheapest_price,
                                        unique_key_pair=unique_key_pair,
                                        unique_security_group=unique_security_group, 
                                        role_instance_profile_item=role_instance_profile_item,
                                        security_group_inbound_rule_items_serialized=security_group_inbound_rule_items_serialized,
                                        spot_master_state_code=spot_master_state_code
                                         )
                # submit CheckStatus msg to check on completion of master resources
                self.send_check_status( spot_master_uuid )
                self.spot_master_sqs_message_durable.delete_message(message)        
            else:
                spot_master_state_code = SpotMasterStateCode.no_instances_available
                unique_key_pair = None               
                unique_security_group = None
                policy = None
                security_group_inbound_rule_items_serialized = None    
                role_instance_profile_item = None
                cheapest_vpc_id = None
                cheapest_subnet_id = None
                cheapest_region_name = None
                cheapest_zone_name = None
                cheapest_price = None
                logger.warning( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'No spot instances currently available, will retry in 5 minutes')
                # At this point, the SpotMasterMessageSubmitBatch message won't be deleted, it will reprocess at the end of the in flight movie
                # change the visibility timeout to 5 minutes
                message.change_visibility( (5*60) )

        except StandardError as e:
            logger.error( str(e) )
            logger.error( traceback.format_exc() )
        
    
    def send_check_status( self, spot_master_uuid ):
        """ Queue a Message to do a CheckStatus on this Master in the near future, i.e. in 5 seconds
            This is the first message that will do CheckStatus to check/transition the Master status, 
            in SpotMasterMessageCheckStatus.process() it will continue to queue up another CheckStatus
            message (with a variable message delay based on the state) until the job completes

        :param spot_master_uuid: 

        """
        spot_master_msg_check_status = SpotMasterMsg( spot_master_uuid=spot_master_uuid, spot_master_msg_type=SpotMasterMsg.TYPE_CHECK_STATUS )
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageCheckStatus )
        self.spot_master_sqs_message_durable.send_message( spot_master_msg_check_status.to_json(), 
                                                           delay_seconds=5,
                                                           message_attributes=message_attributes )
        

    def create_master_row( self, dynamodb_conn, batch_job_parm_item, spot_master_msg=None, spot_master_uuid=None,
                           cheapest_vpc_id=None, cheapest_subnet_id=None, cheapest_region_name=None, 
                           cheapest_zone_name=None, cheapest_price=None,
                           unique_key_pair=None, unique_security_group=None,
                           role_instance_profile_item=None, security_group_inbound_rule_items_serialized=None,
                           spot_master_state_code=SpotMasterStateCode.master_resources_in_progress):
        """ Create item in the Master table in DynamoDB keyed by spot_master_uuid.  This is the primary entry for a job,
            all entries in the Request table have an implied FK relation by the spot_master_uuid

        :param dynamodb_conn: reuse existing DynamoDB connection
        :param batch_job_parm_item: BatchJobParmItem instance
        :param spot_master_msg: Master msg that initiated this job (Default value = None)
        :param spot_master_uuid:  (Default value = None)
        :param cheapest_vpc_id:  (Default value = None)
        :param cheapest_subnet_id:  (Default value = None)
        :param cheapest_region_name:  (Default value = None)
        :param cheapest_zone_name:  (Default value = None)
        :param cheapest_price:  (Default value = None)
        :param unique_key_pair:  (Default value = None)
        :param unique_security_group:  (Default value = None)
        :param role_instance_profile_item:  (Default value = None)
        :param security_group_inbound_rule_items_serialized:  (Default value = None)
        :param spot_master_state_code:  (Default value = SpotMasterStateCode.master_resources_in_progress)

        """
        ts_start = int( time.time() )

        dict_create_master_item = {
                                    TableSpotMaster.spot_master_uuid:spot_master_uuid,
                                    TableSpotMaster.ts_last_state_check:ts_start,
                                    TableSpotMaster.is_open:1,
                                    TableSpotMaster.spot_master_state_code:spot_master_state_code,
                                    TableSpotMaster.ts_start:ts_start,
                                    TableSpotMaster.region_name:self.region_name,
                                    TableSpotMaster.profile_name:self.profile_name,
                                    TableSpotMaster.ami_id:batch_job_parm_item.ami_id,
                                    TableSpotMaster.num_instances:int( batch_job_parm_item.num_instances ),
                                    TableSpotMaster.instance_type:batch_job_parm_item.instance_type,
                                    TableSpotMaster.instance_username:batch_job_parm_item.instance_username,
                                    TableSpotMaster.current_attempt_number:1,
                                    TableSpotMaster.num_requests_complete_ok:0,
                                    TableSpotMaster.sg_id:unique_security_group.id,
                                    TableSpotMaster.kp_name:unique_key_pair.name,
                                    TableSpotMaster.role_name:role_instance_profile_item.role_name,
                                    TableSpotMaster.policy_name:role_instance_profile_item.policy_name,
                                    TableSpotMaster.cheapest_vpc_id:cheapest_vpc_id,
                                    TableSpotMaster.cheapest_subnet_id:cheapest_subnet_id,
                                    TableSpotMaster.cheapest_region_name:cheapest_region_name,
                                    TableSpotMaster.cheapest_zone_name:cheapest_zone_name,
                                    TableSpotMaster.cheapest_price:str(cheapest_price),
                                   }
        # Try to put, if fail then try a few more times
        spot_master_table = Table( self.spot_master_table_name, connection=dynamodb_conn ) 
        result_master_put = spot_master_table.put_item(data=dict_create_master_item)
        if result_master_put: return
            
        put_attempt_cnt = 0
        put_attempt_max = 10
        while True:
            dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
            spot_master_table = Table( self.spot_master_table_name, connection=dynamodb_conn ) 
            result_master_put = spot_master_table.put_item(data=dict_create_master_item)
            if result_master_put: break
            put_attempt_cnt += 1
            if put_attempt_cnt == put_attempt_max: 
                raise awsspotbatch.common.exception.DynamoDbPutItemMaxAttemptsExceeded('Failed attempt to insert item in: ' + self.spot_master_table_name + 
                                                         ' for spot_master_uuid: ' + spot_master_uuid, self.spot_master_table_name )
            time.sleep(6)


