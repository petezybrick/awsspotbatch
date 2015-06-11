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
Common methods for awsspotbatch.microsvc
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

def get_batch_job_parm_item( spot_master_uuid, spot_batch_job_parm_table_name, region_name='us-east-1', 
                             profile_name=None, attributes=None ):
    """

    :param spot_master_uuid: 
    :param spot_batch_job_parm_table_name: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    :param attributes:  (Default value = None)

    """
    get_attempt_cnt = 0
    get_attempt_max = 10
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )

    while True:
        batch_job_parm_item_table = Table( spot_batch_job_parm_table_name, connection=dynamodb_conn ) 
        try:
            batch_job_parm_item = batch_job_parm_item_table.get_item( spot_master_uuid=spot_master_uuid, attributes=attributes )
            return batch_job_parm_item
        except StandardError as e:
            get_attempt_cnt += 1
            if get_attempt_cnt == get_attempt_max: 
                raise awsspotbatch.common.exception.DynamoDbGetItemMaxAttemptsExceeded('Failed attempt to get item from: ' + spot_batch_job_parm_table_name + 
                                                         ' for spot_master_uuid: ' + spot_master_uuid + ' due to exception: ' + e.get_message(), spot_batch_job_parm_table_name )
            dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
            time.sleep(6)


def put_batch_job_parm_item( spot_master_uuid, spot_batch_job_parm_table_name, spot_master_msg, region_name='us-east-1', profile_name=None  ):
    """

    :param spot_master_uuid: 
    :param spot_batch_job_parm_table_name: 
    :param spot_master_msg: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)

    """
    dict_create_batch_job_parm_item = {
                                TableSpotBatchJobParm.spot_master_uuid:spot_master_uuid,
                                TableSpotBatchJobParm.raw_batch_job_parm_item:spot_master_msg.raw_batch_job_parm_item,
                                TableSpotBatchJobParm.raw_user_job_parm_item:spot_master_msg.raw_user_job_parm_item,
                                }
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
    batch_job_parm_item_table = Table( spot_batch_job_parm_table_name, connection=dynamodb_conn ) 
    result_batch_job_parm_item_put = batch_job_parm_item_table.put_item(data=dict_create_batch_job_parm_item)
    if result_batch_job_parm_item_put: return
    
    # First put failed, try a few more times
    max_attempts = 10
    num_attempts = 0
    while True:
        batch_job_parm_item_table = Table( spot_batch_job_parm_table_name, connection=dynamodb_conn ) 
        result_batch_job_parm_item_put = batch_job_parm_item_table.put_item(data=dict_create_batch_job_parm_item)
        if result_batch_job_parm_item_put: break
        num_attempts += 1
        if num_attempts == max_attempts: 
            raise awsspotbatch.common.exception.DynamoDbPutItemMaxAttemptsExceeded('Exceeded put_item attempts on batch_job_parm table, spot_master_uuid=' + spot_master_uuid, spot_batch_job_parm_table_name )
        time.sleep(6)                       
