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
Process the SpotMasterMessageResubmitFailedRequest message
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import uuid
import traceback
import datetime
import boto.dynamodb2
import awsext.ec2
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import TableSpotMaster, TableSpotRequest, TableSpotBatchJobParm, SpotMasterStateCode
from awsspotbatch.microsvc.master.spotmastermessagebase import SpotMasterMessageBase
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.util import create_microsvc_message_attributes
from awsspotbatch.microsvc.master import find_spot_cheapest_prices, fmt_master_item_msg_hdr, fmt_master_uuid_msg_hdr



import logging
logger = logging.getLogger(__name__)


class SpotMasterMessageResubmitFailedRequest(SpotMasterMessageBase):
    """ Process the SpotMasterMessageResubmitFailedRequest message"""

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
        """ Try to submit another Spot Request based on the one that just failed

        :param message: SQS Message instance

        """
        try: 
            spot_master_msg = SpotMasterMsg( raw_json=message.get_body() )
            spot_master_uuid = spot_master_msg.spot_master_uuid       
            logger.info( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'process_resubmit_failed_request')
            dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
            spot_master_table = Table( self.spot_master_table_name, connection=dynamodb_conn ) 
            spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )
            spot_request_table = Table( self.spot_request_table_name, connection=dynamodb_conn ) 
            failed_spot_request_item = spot_request_table.get_item( spot_request_uuid=spot_master_msg.spot_request_uuid )
    
            # Request spot instance
            spot_instance_request = self.resubmit_failed_request_spot_instance( spot_master_item, failed_spot_request_item, dynamodb_conn )
    
            # Queue up a SpotRequestMsg     
            if spot_instance_request != None:
                spot_request_uuid = str(uuid.uuid1())
                spot_request_msg = SpotRequestMsg( spot_request_uuid=spot_request_uuid, 
                                                   spot_master_uuid=spot_master_item[ TableSpotMaster.spot_master_uuid ], 
                                                   spot_request_msg_type=SpotRequestMsg.TYPE_SPOT_REQUEST_INITIATED, 
                                                   spot_request_id=spot_instance_request.id )
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_SPOT_PRICE ] = str( spot_instance_request.price )
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_INSTANCE_USERNAME ] = spot_master_item[ TableSpotMaster.instance_username ]
                spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_ATTEMPT_NUMBER ] = int( failed_spot_request_item[ TableSpotRequest.attempt_number ] + 1 )
                
                spot_request_sqs_message_durable = SqsMessageDurable( self.spot_request_queue_name, self.region_name, profile_name=self.profile_name )
                spot_request_sqs_message_durable.send_message( spot_request_msg.to_json(), message_attributes=create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageSpotRequestInitiated ) )
                self.spot_master_sqs_message_durable.delete_message(message) 
            # No instances available - resubmit this message with a delay timer so it will get reprocessed in future
            else:
                logger.warning( fmt_master_uuid_msg_hdr( spot_master_uuid ) + 'No spot instances available, will try again in ' + str(awsspotbatch.common.const.NO_SPOT_INSTANCES_AVAILABLE_RECHECK_MINUTES) + ' minutes')
                delay_seconds = awsspotbatch.common.const.NO_SPOT_INSTANCES_AVAILABLE_RECHECK_MINUTES * 60
                self.spot_master_sqs_message_durable.send_message( message.get_body(), 
                                                                   message_attributes=create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageResubmitFailedRequest ), 
                                                                   delay_seconds=delay_seconds )
                self.spot_master_sqs_message_durable.delete_message(message)

        except StandardError as e:
            logger.error( fmt_master_item_msg_hdr( spot_master_item ) + str(e) )
            logger.error( fmt_master_item_msg_hdr( spot_master_item ) + traceback.format_exc() )
        
    
    def resubmit_failed_request_spot_instance( self, spot_master_item, failed_spot_request_item, dynamodb_conn, profile_name=None ):
        """ Spot Request failed for whatever reason, try to submit another

        :param spot_master_item: 
        :param failed_spot_request_item: 
        :param dynamodb_conn: 
        :param profile_name:  (Default value = None)
        :return: SpotInstanceRequest if successful, None if not successful (i.e. spot price > max bid)

        """
        spot_batch_job_parm_table = Table(awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME, connection=dynamodb_conn ) 
        spot_batch_job_parm_item = spot_batch_job_parm_table.get_item( spot_master_uuid=spot_master_item[TableSpotMaster.spot_master_uuid] )        
        batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_batch_job_parm_item[TableSpotBatchJobParm.raw_batch_job_parm_item] )
        cheapest_subnet_id, new_bid_price, cheapest_zone_name = self.find_cheapest_subnet_price_after_failure( spot_master_item, failed_spot_request_item, batch_job_parm_item )

        if cheapest_subnet_id != None:
            valid_until = datetime.datetime.utcnow() + datetime.timedelta( minutes=batch_job_parm_item.spot_request_valid_for_minutes )
            valid_until_iso = valid_until.isoformat()
            ec2_conn = awsext.ec2.connect_to_region( spot_master_item[ TableSpotMaster.cheapest_region_name ] )
            ec2_conn.cancel_spot_instance_requests( [ failed_spot_request_item[TableSpotRequest.spot_request_id] ] )
            spot_instance_requests = ec2_conn.request_spot_instances(
                new_bid_price, 
                image_id=batch_job_parm_item.ami_id, 
                count=1, 
                type='one-time', 
                dry_run=batch_job_parm_item.dry_run,
                valid_until=valid_until_iso, 
                user_data=None, 
                instance_type=spot_master_item[ TableSpotMaster.instance_type ],
                placement=cheapest_zone_name, 
                instance_profile_name=spot_master_item[ TableSpotMaster.role_name ],
                key_name=spot_master_item[ TableSpotMaster.kp_name ],
                security_group_ids=[ spot_master_item[ TableSpotMaster.sg_id ] ],
                subnet_id=cheapest_subnet_id,
                availability_zone_group='single',
                )
            
            if spot_instance_requests != None and len(spot_instance_requests) == 1: return spot_instance_requests[0]
            else: return None
        else: return None
    

    def find_cheapest_subnet_price_after_failure( self, spot_master_item, failed_spot_request_item, batch_job_parm_item ):
        """ Find cheapest price based on the region of the original request
            If none can be found or spot price is > max bid, then return none

        :param spot_master_item: 
        :param failed_spot_request_item: 
        :param batch_job_parm_item: 
        :return: cheapest_subnet_id, bid_price, cheapest_zone_name

        """
        bid_price = -1.0
        cheapest_subnet_id = None
        cheapest_zone_name = None
        spot_cheapest_item = None
        spot_cheapest_items = find_spot_cheapest_prices( instance_type=spot_master_item[ TableSpotMaster.instance_type ], 
                                                         product_description='Linux/UNIX', 
                                                         profile_name=spot_master_item[ TableSpotMaster.profile_name ], 
                                                         region_filter=[ spot_master_item[ TableSpotMaster.cheapest_region_name ] ], 
                                                         max_bid=batch_job_parm_item.spot_query_max_bid)
        if len(spot_cheapest_items) > 0:
            spot_cheapest_item = spot_cheapest_items[0]
            spot_query_regions_azs_subnets = batch_job_parm_item.spot_query_regions_azs_subnets
            cheapest_subnet_id = None
            if spot_cheapest_item.region.name in spot_query_regions_azs_subnets:
                azs_subnets = spot_query_regions_azs_subnets[ spot_cheapest_item.region.name ]
                if spot_cheapest_item.zone.name in azs_subnets:
                    cheapest_subnet_id = azs_subnets[ spot_cheapest_item.zone.name ]
                    cheapest_zone_name = spot_cheapest_item.zone.name
                    bid_price = self.calc_new_bid_price_after_failure( spot_cheapest_item.price )
        return cheapest_subnet_id, bid_price, cheapest_zone_name
        
    
    def calc_new_bid_price_after_failure( self, cheapest_price ):
        """ Uplift the cheapest price to minimize probability of termination
            for now, just uplift the failed price by 10%
            
        :param cheapest_price: Cheapest spot price in a given subnet

        """

        new_bid_price = cheapest_price * 1.1
        return str(new_bid_price)

