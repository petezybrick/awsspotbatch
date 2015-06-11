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
Common methods for awsspotbatch.microsvc.master
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import datetime
import boto.dynamodb2
import awsext.ec2
import awsext.iam
import awsext.vpc
import awsspotbatch.common.exception
import awsspotbatch.common.util
from awsext.ec2.spotprice import find_spot_cheapest_prices 
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import TableSpotMaster, SpotMasterStateCode, TableSpotRSAKey,\
    TableSpotBatchJobParm, TableSpotRequest
from awsspotbatch.common.batchjobparmitem import deserialize_inbound_rule_items, BatchJobParmItem
from awsspotbatch.common.util import create_policy


import logging
logger = logging.getLogger(__name__)

  
def spot_master_row_partial_save( spot_master_table_name, spot_master_item, dict_keys_values,  
                                  region_name='us-east-1', profile_name=None ):
    """Save Master item attribute name/values as specified in dict_keys_values

    :param spot_master_table_name: 
    :param spot_master_item: 
    :param dict_keys_values: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)

    """
    for key,value in dict_keys_values.iteritems(): spot_master_item[ key ] = value
    partial_save_result = spot_master_item.partial_save()
    if partial_save_result: return      # success on first partial save attempt
    
    # First partial save failed, try a few more times
    spot_master_uuid =  spot_master_item[ TableSpotMaster.spot_master_uuid ]
    max_attempts = 10
    num_attempts = 0
    while True:
        dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
        spot_master_table = Table( spot_master_table_name, connection=dynamodb_conn ) 
        spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )
        for key,value in dict_keys_values.iteritems(): spot_master_item[ key ] = value
        partial_save_result = spot_master_item.partial_save()
        # partial_save_result = master_item.partial_save()
        if partial_save_result: break
        num_attempts += 1
        if num_attempts == max_attempts: 
            raise awsspotbatch.common.exception.DynamoDbPartialSaveError('Exceeded partial save attempts on master table, spot_master_uuid=' + spot_master_uuid, spot_master_table_name )
        time.sleep(6)                       

def calc_bid_price( cheapest_price ):
    """ Calculate the bid price, for now just uplift by 10%

    :param cheapest_price: 
    :return: cheapest price uplifted by 10%

    """
    
    return cheapest_price * 1.1
    

def find_cheapest_subnet_price( batch_job_parm_item, profile_name=None ):
    """ Find cheapest subnet and price

    :param batch_job_parm_item: 
    :param profile_name:  (Default value = None)
    :return: cheapest_subnet, bid_price, spot_cheapest_item

    """
    bid_price = -1.0
    cheapest_subnet = None
    spot_cheapest_item = None
    spot_cheapest_items = find_spot_cheapest_prices( instance_type=batch_job_parm_item.instance_type, 
                                                     product_description='Linux/UNIX', 
                                                     profile_name=profile_name, 
                                                     region_filter=batch_job_parm_item.spot_query_region_names, 
                                                     max_bid=batch_job_parm_item.spot_query_max_bid)
    if len(spot_cheapest_items) > 0:
        spot_cheapest_item = spot_cheapest_items[0]
        spot_query_regions_azs_subnets = batch_job_parm_item.spot_query_regions_azs_subnets
        cheapest_subnet = None
        if spot_cheapest_item.region.name in spot_query_regions_azs_subnets:
            azs_subnets = spot_query_regions_azs_subnets[ spot_cheapest_item.region.name ]
            if spot_cheapest_item.zone.name in azs_subnets:
                cheapest_subnet = azs_subnets[ spot_cheapest_item.zone.name ]
                bid_price = calc_bid_price( spot_cheapest_item.price )
    return cheapest_subnet, bid_price, spot_cheapest_item


def submit_request_spot_instances( spot_master_item, region_name='us-east-1', profile_name=None ):
    """ Submit initial request for spot instances.

    :param spot_master_item: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    :return: list of SpotInstanceRequest instances

    """
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
    spot_batch_job_parm_table = Table( awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME, connection=dynamodb_conn ) 
    spot_batch_job_parm_item = spot_batch_job_parm_table.get_item( spot_master_uuid=spot_master_item[TableSpotMaster.spot_master_uuid] )        
    batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_batch_job_parm_item[TableSpotBatchJobParm.raw_batch_job_parm_item] )

    valid_until = datetime.datetime.utcnow() + datetime.timedelta( minutes=batch_job_parm_item.spot_request_valid_for_minutes )
    valid_until_iso = valid_until.isoformat()
    ec2_conn = awsext.ec2.connect_to_region( spot_master_item[ TableSpotMaster.cheapest_region_name ] )
    spot_instance_requests = ec2_conn.request_spot_instances(
        spot_master_item[ TableSpotMaster.cheapest_price ], 
        image_id=batch_job_parm_item.ami_id, 
        count=batch_job_parm_item.num_instances, 
        type='one-time', 
        dry_run=batch_job_parm_item.dry_run,
        valid_until=valid_until_iso, 
        user_data=None, 
        instance_type=spot_master_item[ TableSpotMaster.instance_type ],
        placement=spot_master_item[ TableSpotMaster.cheapest_zone_name ], 
        instance_profile_name=spot_master_item[ TableSpotMaster.role_name ],
        key_name=spot_master_item[ TableSpotMaster.kp_name ],
        security_group_ids=[ spot_master_item[ TableSpotMaster.sg_id ] ],
        subnet_id=spot_master_item[ TableSpotMaster.cheapest_subnet_id ],
        availability_zone_group='single',
        )
    
    # set the name on each spot request to the master uuid
    cnt_attempts= 0
    tags = { 'Name':'spot_master_' + spot_master_item[TableSpotMaster.spot_master_uuid] }
    for spot_instance_request in spot_instance_requests:
        while True:
            try:
                ec2_conn.create_tags(spot_instance_request.id, tags )
                break;
            except:
                cnt_attempts += 1
                if cnt_attempts == 100:
                    logger.error( fmt_master_item_msg_hdr( spot_master_item ) + 'Could not set Name tag on spot_instance_request')
                    break
                time.sleep( .25 )
                pass
         
    return spot_instance_requests


def put_rsa_key_item( spot_master_uuid, spot_rsa_key_table_name, rsa_key_encoded, region_name='us-east-1', profile_name=None ):
    """ Create RSA Key item in RSA Key table, this will be used later to SSH into Spot instances

    :param spot_master_uuid: 
    :param spot_rsa_key_table_name: 
    :param rsa_key_encoded: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)

    """
    dict_create_rsa_key_item = {
                                TableSpotRSAKey.spot_master_uuid:spot_master_uuid,
                                TableSpotRSAKey.rsa_key_encoded:rsa_key_encoded,
                                }
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
    spot_rsa_key_table = Table( spot_rsa_key_table_name, connection=dynamodb_conn ) 
    result_rsa_key_put = spot_rsa_key_table.put_item(data=dict_create_rsa_key_item)
    if result_rsa_key_put: return
    
    # First put failed, try a few more times
    max_attempts = 10
    num_attempts = 0
    while True:
        spot_rsa_key_table = Table( spot_rsa_key_table_name, connection=dynamodb_conn ) 
        result_rsa_key_put = spot_rsa_key_table.put_item(data=dict_create_rsa_key_item)
        if result_rsa_key_put: break
        num_attempts += 1
        if num_attempts == max_attempts: 
            raise awsspotbatch.common.exception.DynamoDbPutItemMaxAttemptsExceeded('Exceeded put_item attempts on rsa_key table, spot_master_uuid=' + spot_master_uuid, spot_rsa_key_table_name )
        time.sleep(6) 
   

def find_spot_request_instance_ids_by_master_uuid( spot_master_uuid, region_name='us-east-1', profile_name=None ):
    """ Based on the Master UUID, return list of instance id's

    :param spot_master_uuid: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    :return: list of spot instance id's

    """
    instance_ids = [] 
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )                
    request_table = Table( awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, connection=dynamodb_conn ) 
    spot_request_items = request_table.query_2( spot_master_uuid__eq=spot_master_uuid, index='MasterUuid' )
    for spot_request_item in spot_request_items:
        if spot_request_item[ TableSpotRequest.instance_id ] != None:
            instance_ids.append(spot_request_item[ TableSpotRequest.instance_id ])
    return instance_ids     


def fmt_master_item_msg_hdr( spot_master_item ):
    """ Helper method for formatting log messages, ensures the Master UUID is always in the log message

    :param spot_master_item: 

    """
    return ' spot_master_uuid=' + spot_master_item[TableSpotMaster.spot_master_uuid] + ' '


def fmt_master_uuid_msg_hdr( spot_master_uuid ):
    """ Helper method for formatting log messages, ensures the Master UUID is always in the log message

    :param spot_master_uuid: 

    """
    return ' spot_master_uuid=' + spot_master_uuid + ' '