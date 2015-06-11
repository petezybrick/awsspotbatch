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
Thread classes to check on status of executing client program and termination detection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import threading
import time
import subprocess
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.msg import SpotRequestMsg
from awsspotbatch.common.util import create_microsvc_message_attributes


# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html 
cmdMetadataTerminateTime = 'if curl -s http://169.254.169.254/latest/meta-data/spot/termination-time | grep -q .*T.*Z; then echo terminating; fi'
heartbeat_interval_seconds = 30
check_termination_time_interval_seconds = 5
   
import logging
logger = logging.getLogger(__name__)


class SpotInstanceTerminationTimeThread(threading.Thread):
    """Check for 2 Minute Warning - AWS has decided to terminate this instance """
    
    def __init__(self, spot_instance_status_thread ):
        """

        :param spot_instance_status_thread: 

        """
        threading.Thread.__init__(self)
        self.spot_instance_status_thread = spot_instance_status_thread
        self.test_cnt = 0
        self.is_shutdown = False
        
        
    def shutdown(self):
        """ """
        self.is_shutdown = True
                
    
    def run(self):
        """Check metadata to determine if AWS is terminating the instance or some other exception encountered """
        num_contiguous_errors = 0
        max_contiguous_errors = 10  # if popen fails 10 times in a row, then save last exception/error and exit
        while True:
            if self.is_shutdown: break
            time.sleep( check_termination_time_interval_seconds )
            if self.is_shutdown: break
            
            try:
                # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html
                process = subprocess.Popen( cmdMetadataTerminateTime, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
                std_out, std_err = process.communicate()
                returncode = process.returncode
                if len(std_err) > 0:
                    num_contiguous_errors += 1;
                    if num_contiguous_errors < max_contiguous_errors: continue
                    self.spot_instance_status_thread.termination_time_exception = 'SpotTerminatedTimeCheckDataError, std_err=' + std_err + ', std_out=' + std_out
                    break
                elif returncode != 0:
                    num_contiguous_errors += 1;
                    if num_contiguous_errors < max_contiguous_errors: continue
                    self.spot_instance_status_thread.termination_time_exception = 'SpotTerminatedTimeCheckReturnCodeError, returncode=' + returncode + ', std_err=' + std_err + ', std_out=' + std_out
                    break
                elif len(std_out) > 0 and std_out.rstrip() == 'terminating':
                    self.spot_instance_status_thread.ts_pending_termination_detected = int( time.time() )
                    break;
                elif len(std_out) > 0:
                    num_contiguous_errors += 1;
                    if num_contiguous_errors < max_contiguous_errors: continue
                    self.spot_instance_status_thread.termination_time_exception = 'SpotTerminatedTimeCheckDataError, std_err=' + std_err + ', std_out=' + std_out
                else:
                    num_contiguous_errors = 0
                
            except OSError as ose:
                self.spot_instance_status_thread.termination_time_exception = str(ose)
                break;
            except ValueError as ve:
                self.spot_instance_status_thread.termination_time_exception = str(ve)
                break;
            except StandardError as se:
                self.spot_instance_status_thread.termination_time_exception = str(se)
                break;            
            

class SpotInstanceStatusThread(threading.Thread):
    """ """
    
    def __init__(self, spot_request_queue_name, region_name, profile_name=None, spot_request_uuid=None, spot_master_uuid=None, spot_request_id=None,  ):
        """

        :param spot_request_queue_name: 
        :param region_name: 
        :param profile_name:  (Default value = None)
        :param spot_request_uuid:  (Default value = None)
        :param spot_master_uuid:  (Default value = None)
        :param spot_request_id:  (Default value = None)

        """
        threading.Thread.__init__(self)
        if spot_request_uuid == None: raise ValueError('spot_request_uuid is required')
        if spot_master_uuid == None: raise ValueError('spot_master_uuid is required')
        if spot_request_id == None: raise ValueError('spot_request_id is required')
        self.spot_request_queue_durable = SqsMessageDurable( spot_request_queue_name, region_name, profile_name=profile_name )
        self.spot_request_uuid = spot_request_uuid
        self.spot_master_uuid = spot_master_uuid
        self.spot_request_id = spot_request_id
        self.spot_instance_termination_time_thread = SpotInstanceTerminationTimeThread( self )
        self.spot_instance_termination_time_thread.start()       
        self.ts_pending_termination_detected = 0
        self.termination_time_exception = None
        self.is_shutdown = False
        self.test_cnt = 0
        
        
    def shutdown(self):
        self.is_shutdown = True
    
    
    def run(self):
        """Loop to determine if client job is done or termination detected.  Send heartbeat at regular interval """
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceHeartbeatDaemonStarted )
        self.spot_request_queue_durable.send_message( SpotRequestMsg( spot_request_uuid=self.spot_request_uuid, 
                                                                      spot_master_uuid=self.spot_master_uuid, 
                                                                      spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_HEARTBEAT_DAEMON_STARTED, 
                                                                      spot_request_id=self.spot_request_id,
                                                                      name_value_pairs={SpotRequestMsg.PAIR_NAME_INSTANCE_HEARTBEAT_DAEMON_STARTED_TIMESTAMP:int(time.time()) },
                                                                      ).to_json(),
                                                     message_attributes=message_attributes )
        wakeup_at_heartbeat = time.time() + heartbeat_interval_seconds
        while True: 
            if self.is_shutdown:
                logging.info('Shutting down') 
                break;
            time.sleep( check_termination_time_interval_seconds )
            if self.is_shutdown:
                logging.info('Shutting down') 
                break;
            if time.time() > wakeup_at_heartbeat:
                wakeup_at_heartbeat = time.time() + heartbeat_interval_seconds
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceHeartbeat )
                self.spot_request_queue_durable.send_message( SpotRequestMsg( spot_request_uuid=self.spot_request_uuid, 
                                                                      spot_master_uuid=self.spot_master_uuid, 
                                                                      spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_HEARTBEAT, 
                                                                      spot_request_id=self.spot_request_id, 
                                                                      name_value_pairs={SpotRequestMsg.PAIR_NAME_INSTANCE_HEARTBEAT_TIMESTAMP:int(time.time()) },
                                                                      ).to_json(),
                                                              message_attributes=message_attributes )
            if self.ts_pending_termination_detected > 0:
                # Instance is going to be terminated within 2 minutes
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstancePendingTerminationDetected )
                self.spot_request_queue_durable.send_message( SpotRequestMsg( spot_request_uuid=self.spot_request_uuid, 
                                                                      spot_master_uuid=self.spot_master_uuid, 
                                                                      spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_PENDING_TERMINATION_DETECTED, 
                                                                      spot_request_id=self.spot_request_id,
                                                                      name_value_pairs={SpotRequestMsg.PAIR_NAME_INSTANCE_TS_PENDING_TERMINATION_DETECTED:self.ts_pending_termination_detected} ).to_json(),
                                                              message_attributes=message_attributes  )
                break
            elif self.termination_time_exception != None:
                # Exception occured while checking the metadata - should never happen, but ...
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstancePendingTerminationException )
                self.spot_request_queue_durable.send_message( SpotRequestMsg( spot_request_uuid=self.spot_request_uuid, 
                                                                      spot_master_uuid=self.spot_master_uuid, 
                                                                      spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_PENDING_TERMINATION_EXCEPTION, 
                                                                      spot_request_id=self.spot_request_id,
                                                                      name_value_pairs={SpotRequestMsg.PAIR_NAME_INSTANCE_TERMINATION_TIME_EXCEPTION:self.termination_time_exception} ).to_json(),
                                                              message_attributes=message_attributes  )
                break
        self.spot_instance_termination_time_thread.shutdown()

