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
Test user batch script on EC2 instance - verify everything works on an on-demand EC2 instance before running on spot instance
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import logging
import threading
import boto.dynamodb2
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.microsvc.request import launch_remote_client
from awsspotbatch.common.tabledef import TableSpotRequest, TableSpotBatchJobParm
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem

logger = logging.getLogger(__name__)


class SSHTestThread(threading.Thread):
    """ """
                                
    def __init__( self, spot_request_uuid ):
        """

        :param spot_request_uuid: 

        """
        threading.Thread.__init__(self)
        self.spot_request_uuid = spot_request_uuid
        
    def run(self):
        """ """
        region_name = 'us-east-1'
        profile_name = 'ipc-training'
        dummy_message = None
        dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )

        spot_request_table = Table( awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, connection=dynamodb_conn ) 
        spot_request_item = spot_request_table.get_item( spot_request_uuid=self.spot_request_uuid )
#         spot_batch_job_parm_table = Table( awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME, connection=dynamodb_conn ) 
#         spot_batch_job_parm_item = spot_batch_job_parm_table.get_item( spot_master_uuid=spot_request_item[TableSpotRequest.spot_master_uuid] )
#         batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_batch_job_parm_item[TableSpotBatchJobParm.raw_batch_job_parm_item] )
        
        client_bootstrap_service_cmds_results, client_bootstrap_user_cmds_results = launch_remote_client( spot_request_item )
        logger.info( 'spot_request_uuid: ' + self.spot_request_uuid )    
        for cmd_result in client_bootstrap_service_cmds_results:
            logger.info( '   service cmd: ' + cmd_result['cmd'])    
            logger.info( '      remote_exit_status: ' + str(cmd_result['remote_exit_status']) )    
            logger.info( '      buf_std_out: ' + cmd_result['buf_std_out'] )    
            logger.info( '      buf_std_err: ' + cmd_result['buf_std_err'] )    
        for cmd_result in client_bootstrap_user_cmds_results:
            logger.info( '   user cmd: ' + cmd_result['cmd'])    
            logger.info( '      remote_exit_status: ' + str(cmd_result['remote_exit_status']) )    
            logger.info( '      buf_std_out: ' + cmd_result['buf_std_out'] )    
            logger.info( '      buf_std_err: ' + cmd_result['buf_std_err'] )    

    
def main():     
    """ """
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    
    try:
        logger.info( 'Starting' )
        spot_request_uuids = ['5527f0e0-eb99-11e4-8c34-101f74edff46', ]
        ssh_test_threads = []
        for spot_request_uuid in spot_request_uuids: ssh_test_threads.append( SSHTestThread( spot_request_uuid ) )
        for ssh_test_thread in ssh_test_threads: ssh_test_thread.start()                                                                  
        for ssh_test_thread in ssh_test_threads: ssh_test_thread.join()                                                                  
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()