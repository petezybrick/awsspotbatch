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
Master and Request Messages
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import json


class SpotMasterMsg(object):
    """ Master Message - used to launch a Master microservice via the Master Dispatcher (AWS Lambda in the future)
        The message header contains the microservice name 
    """
    TYPE_SUBMIT_BATCH = 'submit_batch'
    TYPE_CHECK_STATUS = 'check_status'
    TYPE_INCR_INSTANCE_SUCCESS_CNT = 'incr_instance_success_cnt'  # increment the number of successfully completed spot instances
    TYPE_RESUBMIT_FAILED_REQUEST = 'resubmit_failed_request'   # spot request failed - contraint, terminated, etc., modify and resubmit
    

    def __init__(self, spot_master_uuid=None, spot_master_msg_type=None, spot_request_uuid=None, 
                 raw_batch_job_parm_item='', raw_user_job_parm_item='', raw_json=None):
        """

        :param spot_master_uuid: master uuid (Default value = None)
        :param spot_master_msg_type: message type - see TYPE_... above (Default value = None)
        :param spot_request_uuid: request uuid (Default value = None)
        :param raw_batch_job_parm_item: string representation of BatchParmItem json (Default value = '')
        :param raw_user_job_parm_item: string representation of UserJobParmItem json   (Default value = '')
        :param raw_json: string representation of json, contains an entire inbound message  (Default value = None)

        """
        if spot_master_uuid != None and spot_master_msg_type != None:
            # Outbound - Create a message to be sent
            self.spot_master_uuid = spot_master_uuid
            self.spot_master_msg_type = spot_master_msg_type
            self.spot_request_uuid = spot_request_uuid
            self.raw_batch_job_parm_item = raw_batch_job_parm_item
            self.raw_user_job_parm_item = raw_user_job_parm_item
        elif raw_json != None:
            # Inbound - Received a dictionary in json - convert to Message
            json_dict = json.loads( raw_json )
            self.spot_master_uuid = json_dict['spot_master_uuid']
            self.spot_master_msg_type = json_dict['spot_master_msg_type']
            self.spot_request_uuid = json_dict['spot_request_uuid']
            self.raw_batch_job_parm_item = json_dict['raw_batch_job_parm_item']
            self.raw_user_job_parm_item = json_dict['raw_user_job_parm_item']
        else: raise ValueError("Invalid parms: must be spot_master_uuid and spot_master_msg_type, or raw_json")
            
        
    def to_json(self):
        """ """
        json_dict = { 
                     "spot_master_uuid":self.spot_master_uuid,
                     "spot_master_msg_type":self.spot_master_msg_type,
                     "spot_request_uuid":self.spot_request_uuid,
                     "raw_batch_job_parm_item":self.raw_batch_job_parm_item,
                     "raw_user_job_parm_item":self.raw_user_job_parm_item,
                     }
        return json.dumps(json_dict)
        

class SpotRequestMsg(object):
    """ Request Message - used to launch a Request microservice via the Request Dispatcher (AWS Lambda in the future)
        The message header contains the microservice name 
    """    
    TYPE_CHECK_STATUS = 'check_status'
    TYPE_RESUBMIT_SPOT_REQUEST = 'resubmit_spot_request'
    TYPE_SPOT_REQUEST_INITIATED = 'spot_request_initiated'
    TYPE_INSTANCE_HEARTBEAT = 'instance_heartbeat'
    TYPE_INSTANCE_HEARTBEAT_DAEMON_STARTED = 'instance_heartbeat_daemon_started'
    TYPE_INSTANCE_PENDING_TERMINATION_DETECTED = 'instance_pending_termination_detected'
    TYPE_INSTANCE_PENDING_TERMINATION_EXCEPTION = 'instance_pending_termination_exception'
    TYPE_INSTANCE_BATCH_PROCESS_COMPLETE = 'instance_batch_process_complete'
    TYPE_INSTANCE_BATCH_PROCESS_START_EXCEPTION = 'instance_batch_process_start_exception'
       
    PAIR_NAME_SPOT_PRICE = 'spot_price'
    PAIR_NAME_BATCH_PROCESS_COMPLETE_TIMESTAMP = 'batch_process_complete_timestamp'
    PAIR_NAME_BATCH_PROCESS_RETURNCODE = 'batch_process_returncode'
    PAIR_NAME_BATCH_PROCESS_STD_OUT = 'batch_process_std_out'
    PAIR_NAME_BATCH_PROCESS_STD_ERR = 'batch_process_std_err'
    PAIR_NAME_INSTANCE_TS_PENDING_TERMINATION_DETECTED = 'instance_ts_pending_termination_detected'
    PAIR_NAME_INSTANCE_TERMINATION_TIME_EXCEPTION = 'instance_termination_time_exception'
    PAIR_NAME_INSTANCE_HEARTBEAT_TIMESTAMP = 'instance_heartbeat_timestamp'
    PAIR_NAME_INSTANCE_HEARTBEAT_DAEMON_STARTED_TIMESTAMP  = 'instance_heartbeat_daemon_started_timestamp'
    PAIR_NAME_INSTANCE_USERNAME  = 'instance_username'
    PAIR_NAME_INSTANCE_BATCH_PROCESS_START_EXCEPTION_MESSAGE  = 'instance_batch_process_start_exception_message'
    PAIR_NAME_INSTANCE_BATCH_PROCESS_START_EXCEPTION_TRACEBACK  = 'instance_batch_process_start_exception_traceback'
    PAIR_NAME_ATTEMPT_NUMBER  = 'attempt_number'


    def __init__(self, spot_request_uuid=None, spot_master_uuid=None, spot_request_msg_type=None, spot_request_id=None, name_value_pairs={}, raw_json=None):
        """

        :param spot_request_uuid: request uuid (Default value = None)
        :param spot_master_uuid: master uuid (Default value = None)
        :param spot_request_msg_type: request type, set TYPE_... above (Default value = None)
        :param spot_request_id: spot request id (i.e. sri-a1b2c3d4) (Default value = None)
        :param name_value_pairs: additional name=value pairs based on the microservice being called (Default value = {})
        :param raw_json: string representation of json, contains an entire inbound message (Default value = None)

        """
        
        if spot_request_uuid != None and spot_master_uuid != None and spot_request_msg_type != None and spot_request_id != None:
            self.spot_request_uuid = spot_request_uuid
            self.spot_master_uuid = spot_master_uuid
            self.spot_request_msg_type = spot_request_msg_type
            self.spot_request_id = spot_request_id
            self.name_value_pairs = name_value_pairs
        elif raw_json != None:
            json_dict = json.loads( raw_json )
            self.spot_request_uuid = json_dict['spot_request_uuid']
            self.spot_master_uuid = json_dict['spot_master_uuid']
            self.spot_request_msg_type = json_dict['spot_request_msg_type']
            self.spot_request_id = json_dict['spot_request_id']
            self.name_value_pairs = json_dict['name_value_pairs']
        else: raise ValueError("Invalid parms: must be spot_request_uuid/spot_master_uuid/spot_request_msg_type/spot_request_id, or raw_json")
            
        
    def to_json(self):
        """ """
        json_dict = { 
                     "spot_request_uuid":self.spot_request_uuid,
                     "spot_master_uuid":self.spot_master_uuid,
                     "spot_request_msg_type":self.spot_request_msg_type,
                     "spot_request_id":self.spot_request_id,
                     "name_value_pairs":self.name_value_pairs,
                     }
        return json.dumps(json_dict)
        