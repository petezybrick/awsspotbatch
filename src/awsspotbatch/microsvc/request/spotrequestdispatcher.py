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
Spot Master Dispatcher.  Note this is an interim solution until AWS Lambda supports SQS
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import threading
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
# CRITICAL: these imports must be here for reflection to work based on the class names
from awsspotbatch.microsvc.request.spotrequestmessagecheckstatus import SpotRequestMessageCheckStatus
from awsspotbatch.microsvc.request.spotrequestmessageinstancebatchprocesscomplete import SpotRequestMessageInstanceBatchProcessComplete
from awsspotbatch.microsvc.request.spotrequestmessageinstancebatchprocessstartexception import SpotRequestMessageInstanceBatchProcessStartException
from awsspotbatch.microsvc.request.spotrequestmessageinstanceheartbeat import SpotRequestMessageInstanceHeartbeat
from awsspotbatch.microsvc.request.spotrequestmessageinstanceheartbeatdaemonstarted import SpotRequestMessageInstanceHeartbeatDaemonStarted
from awsspotbatch.microsvc.request.spotrequestmessageinstancependingterminationdetected import SpotRequestMessageInstancePendingTerminationDetected
from awsspotbatch.microsvc.request.spotrequestmessagespotrequestinitiated import SpotRequestMessageSpotRequestInitiated
from awsspotbatch.microsvc.request.spotrequestmessageinstancependingterminationexception import SpotRequestMessageInstancePendingTerminationException


import logging
logger = logging.getLogger(__name__)


class SpotRequestDispatcher(threading.Thread):
    """ 
        Dispatch Request messages - launch a microservice to execute based on the service_class_name attribute
        Note that the service class names are defined in :class:awsspotbatch.common.const and begin with MICROSVC_REQUEST_CLASSNAME_    
    """
                                
    def __init__( self, 
                  spot_master_table_name=awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, 
                  spot_master_queue_name=awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, 
                  spot_request_table_name=awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, 
                  spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, 
                  spot_rsa_key_table_name=awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, 
                  spot_batch_job_parm_table_name=awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME,
                  region_name='us-east-1', 
                  profile_name=None 
                ):
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
        threading.Thread.__init__(self)
        self.region_name = region_name
        self.profile_name = profile_name
        self.spot_master_table_name = spot_master_table_name
        self.spot_master_queue_name = spot_master_queue_name
        self.spot_request_table_name = spot_request_table_name
        self.spot_request_queue_name = spot_request_queue_name
        self.spot_rsa_key_table_name = spot_rsa_key_table_name
        self.spot_batch_job_parm_table_name = spot_batch_job_parm_table_name
        self.is_shutdown = False


    def run(self):
        """ Read Request messages, launch Request microservice based on service_class_name message attribute """
        try:
            logger.info( 'Starting:' )
            spot_request_sqs_message_durable = SqsMessageDurable( self.spot_request_queue_name, self.region_name, profile_name=self.profile_name )
            while True:
                logger.info('SpotRequestDispatcher loop')
                if self.is_shutdown: 
                    logger.info('Shutting down SpotRequestDispatcher' )
                    break;
                
                message = spot_request_sqs_message_durable.receive_message( message_attributes=['service_class_name'])
                if message == None: continue
                
                message_attribute = message.message_attributes['service_class_name']
                service_class_name = message_attribute['string_value']
                logger.info('Launching ' +  service_class_name )
                SpotRequestMicrosvcLauncher( service_class_name, message, self ).start()
    
        except StandardError as e:
            logger.error('Exiting SpotRequestDispatcher due to exception'  )
            logger.error( e )
            logger.error( traceback.format_exc() )


class SpotRequestMicrosvcLauncher(threading.Thread):
    """ Launch and run a Request microservice based on the service_class_name """
    
    def __init__( self, service_class_name, message, spot_request_dispatcher
                ):
        """

        :param service_class_name: Service Class Name from the message attribute service_class_name
        :param message: raw json of SpotRequestMessage instance
        :param spot_request_dispatcher: spot request dispatcher instance - contains various attributes necessary to launch the microservice

        """
        threading.Thread.__init__(self)
        self.service_class_name = service_class_name
        self.message = message
        self.spot_request_dispatcher = spot_request_dispatcher
        
    
    def run(self):        
        """ Instantiate via reflection and run the microservice"""
        constructor = globals()[self.service_class_name]
        instance = constructor(                   
                               spot_master_table_name=self.spot_request_dispatcher.spot_master_table_name, 
                               spot_master_queue_name=self.spot_request_dispatcher.spot_master_queue_name,  
                               spot_request_table_name=self.spot_request_dispatcher.spot_request_table_name, 
                               spot_request_queue_name=self.spot_request_dispatcher.spot_request_queue_name, 
                               spot_rsa_key_table_name=self.spot_request_dispatcher.spot_rsa_key_table_name, 
                               spot_batch_job_parm_table_name=self.spot_request_dispatcher.spot_batch_job_parm_table_name, 
                               region_name=self.spot_request_dispatcher.region_name, 
                               profile_name=self.spot_request_dispatcher.profile_name  )
        instance.process( self.message )
