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
Create and Test all DynamoDB tables - Master, Request, Parm and RSAKey
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import time
import uuid
import boto.dynamodb2
from awsspotbatch.common.tabledef import TableSpotMaster, TableSpotRequest, SpotMasterStateCode, SpotRequestStateCode, TableSpotRSAKey,\
    TableSpotBatchJobParm
from awsext.dynamodb import delete_and_wait_until_tables_deleted, wait_until_tables_active
from boto.dynamodb2.fields import HashKey, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING, BOOLEAN, NUMBER
import awsspotbatch.common.const

import logging
logger = logging.getLogger(__name__)


class TableMgr(object):
    """Create and Test all DynamoDB tables - Master, Request, Parm and RSAKey"""


    def __init__(self, region_name, profile_name=None, 
                spot_master_table_name=awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, 
                spot_request_table_name=awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME,
                spot_rsa_key_table_name=awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, 
                spot_batch_job_parm_table_name=awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME,
                  ):
        """

        :param region_name: 
        :param profile_name:  (Default value = None)
        :param spot_master_table_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME)
        :param spot_request_table_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME)
        :param spot_rsa_key_table_name:  (Default value = awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME)
        :param spot_batch_job_parm_table_name:  (Default value = awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME)

        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.spot_master_table_name = spot_master_table_name
        self.spot_request_table_name = spot_request_table_name
        self.spot_rsa_key_table_name = spot_rsa_key_table_name
        self.spot_batch_job_parm_table_name = spot_batch_job_parm_table_name
        self.dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
        self.spot_master_table = None
        self.spot_request_table = None     
        self.spot_rsa_key_table = None     
        self.spot_batch_job_parm_table = None     


    def create_tables( self ):         
        """ """
        try:
            logger.info( 'Starting' )                           
            # if the table exists then delete it
            self.spot_master_table = Table( self.spot_master_table_name, connection=self.dynamodb_conn ) 
            self.spot_request_table = Table( self.spot_request_table_name, connection=self.dynamodb_conn ) 
            self.spot_rsa_key_table = Table( self.spot_rsa_key_table_name, connection=self.dynamodb_conn ) 
            self.spot_batch_job_parm_table = Table( self.spot_batch_job_parm_table_name, connection=self.dynamodb_conn ) 
            
            delete_and_wait_until_tables_deleted( [self.spot_master_table, self.spot_request_table, 
                                                   self.spot_rsa_key_table, self.spot_batch_job_parm_table ] )
    
            self.spot_master_table = Table.create( self.spot_master_table_name, connection=self.dynamodb_conn, 
                                                   schema=[HashKey( TableSpotMaster.spot_master_uuid, data_type=STRING) ],
                                                   global_indexes=[ GlobalAllIndex('IsOpen', parts=[ HashKey( TableSpotMaster.is_open, data_type=NUMBER) ] ) ] 
                                                   )
            self.spot_request_table = Table.create( self.spot_request_table_name, connection=self.dynamodb_conn, 
                                               schema=[HashKey( TableSpotRequest.spot_request_uuid, data_type=STRING) ],
                                               global_indexes=[ GlobalAllIndex('MasterUuid', parts=[ HashKey( TableSpotRequest.spot_master_uuid, data_type=STRING) ] ) ] 
                                               )
            self.spot_rsa_key_table = Table.create( self.spot_rsa_key_table_name, connection=self.dynamodb_conn, 
                                         schema=[HashKey( TableSpotRSAKey.spot_master_uuid, data_type=STRING) ] 
                                         )          
            self.spot_batch_job_parm_table = Table.create( self.spot_batch_job_parm_table_name, connection=self.dynamodb_conn, 
                                         schema=[HashKey( TableSpotBatchJobParm.spot_master_uuid, data_type=STRING) ] 
                                         )          
            
            wait_until_tables_active( [self.spot_master_table, self.spot_request_table, self.spot_rsa_key_table, self.spot_batch_job_parm_table ] )            
            logger.info( 'Completed Successfully' )
    
        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            sys.exit(8)
    
    
    def create_test_data(  self  ):         
        """ """
        try:
            if self.spot_master_table == None:
                self.spot_master_table = Table( self.spot_master_table_name, connection=self.dynamodb_conn ) 
            if self.spot_request_table == None:
                self.spot_request_table = Table( self.spot_request_table_name, connection=self.dynamodb_conn ) 
            if self.spot_rsa_key_table == None:
                self.spot_rsa_key_table = Table( self.spot_rsa_key_table_name, connection=self.dynamodb_conn ) 
            
            spot_master_uuid = str( uuid.uuid1() )
            logger.info('uuid=' + spot_master_uuid)
            # These values would already exist
            ts_start = int( time.time() )
            num_instances = 3
            instance_type = 'm3.xlarge'
            sg_id = 'sg_t1'
            kp_name = 'kp_t1'
            role_name = 'role_t1'
            spot_price = str(0.0123)
            attempt_number = 1      # increments each time a spot request fails and a new spot is requested
    
            dict_create_master_item = {
                                        TableSpotMaster.spot_master_uuid:spot_master_uuid,
                                        TableSpotMaster.ts_last_state_check:ts_start,
                                        TableSpotMaster.is_open:1,
                                        TableSpotMaster.spot_master_state_code:SpotMasterStateCode.batch_submit,
                                        TableSpotMaster.ts_start:ts_start,
                                        TableSpotMaster.region_name:self.region_name,
                                        TableSpotMaster.num_instances:num_instances,
                                        TableSpotMaster.instance_type:instance_type,
                                        TableSpotMaster.current_attempt_number:attempt_number,
                                        TableSpotMaster.sg_id:sg_id,
                                        TableSpotMaster.kp_name:kp_name,
                                        TableSpotMaster.role_name:role_name,
                                       }
    
            result_master_put = self.spot_master_table.put_item(data=dict_create_master_item)
            logger.info( 'result_master_put:' + str(result_master_put) )
                    
            # Create 3x simulated spot instance requests - simulate that they are already granted and StateFlowMaster has changed state code from pending_start to running
            spot_request_ids = []
            now = str( int(time.time() ))
            for i in range(0,3): spot_request_ids.append( 'sri-' + str(i) + '-' + now )

            for spot_request_id in spot_request_ids:
                dict_create_spot_request_item = {
                                            TableSpotRequest.spot_request_uuid:str( uuid.uuid1() ),
                                            TableSpotRequest.spot_master_uuid:spot_master_uuid,
                                            TableSpotRequest.spot_request_id:spot_request_id,
                                            TableSpotRequest.ts_last_state_check:ts_start,
                                            TableSpotRequest.attempt_number:attempt_number,
                                            TableSpotRequest.spot_price:spot_price,
                                            TableSpotRequest.is_open:1,
                                            TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_running,
                                            TableSpotRequest.ts_start:ts_start,
                                           }
                result_request_put = self.spot_request_table.put_item(data=dict_create_spot_request_item)
                logger.info( 'result_request_put:' + str(result_request_put) )
    
            # Create simulated RSA key
            dict_create_rsa_key_item = {
                                        TableSpotRSAKey.spot_master_uuid:spot_master_uuid,
                                        TableSpotRSAKey.rsa_key_encoded:'aaabbbccc',
                                       }
    
            result_rsa_key_put = self.spot_rsa_key_table.put_item(data=dict_create_rsa_key_item)
            logger.info( 'result_rsa_key_put:' + str(result_rsa_key_put) )
    
        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            sys.exit(8)
