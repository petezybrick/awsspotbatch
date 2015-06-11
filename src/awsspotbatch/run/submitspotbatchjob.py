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
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem

"""
Submit a users' spot batch job
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import uuid
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.msg import SpotMasterMsg
from awsspotbatch.common.util import create_microsvc_message_attributes


def submit_spot_batch_job( argv ):
    """ Submit a users' spot batch job
        Submit an SQS message containing the 2 parm files - Batch Job and User Parm

    :param argv: 

    """
    import logging.config
    if len(sys.argv) == 1:
        print 'ERROR: Missing log configuration file, first argument must be path/name.ext of the log configuration file'
        sys.exit(8)
    logging.config.fileConfig( sys.argv[1], disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    if len(sys.argv) == 2:
        logger.error( 'ERROR: Missing Batch Job Parm file, second argument must be path/name.ext of the log Batch Job Parm file' )
        sys.exit(8)              
    
    try:
        logger.info("Starting")
        
        path_batch_job_parm_file = sys.argv[2]
        if len(sys.argv) == 4: path_user_job_parm_file = sys.argv[3]
        else: path_user_job_parm_file = None
        
        with open( path_batch_job_parm_file ) as parm_file:
            raw_batch_job_parm_item = parm_file.read()
            
        if path_user_job_parm_file != None:   
            with open( path_user_job_parm_file ) as parm_file:
                raw_user_job_parm_item = parm_file.read()
        else: raw_user_job_parm_item = None

        batch_job_parm_item = BatchJobParmItem( stringParmFile=raw_batch_job_parm_item )

        spot_master_sqs_message_durable = SqsMessageDurable( awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, 
                                                             batch_job_parm_item.primary_region_name, 
                                                             profile_name=batch_job_parm_item.profile_name )
 
        spot_master_uuid = str(uuid.uuid1())
        logger.info('Submitting test batch message, spot_master_uuid=' + spot_master_uuid )
        spot_master_msg = SpotMasterMsg( spot_master_uuid=spot_master_uuid, spot_master_msg_type=SpotMasterMsg.TYPE_SUBMIT_BATCH,
                                         raw_batch_job_parm_item=raw_batch_job_parm_item, raw_user_job_parm_item=raw_user_job_parm_item)
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageSubmitBatch )
        spot_master_sqs_message_durable.send_message( spot_master_msg.to_json(),
                                                      message_attributes=message_attributes )
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)


if __name__ == '__main__':
    submit_spot_batch_job( sys.argv )
    