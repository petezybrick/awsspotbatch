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
Constants
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

SPOT_MASTER_TABLE_NAME = 'spotbatch.spotmaster'
SPOT_REQUEST_TABLE_NAME = 'spotbatch.spotrequest'
SPOT_RSA_KEY_TABLE_NAME = 'spotbatch.spotrsakey'
SPOT_BATCH_JOB_PARM_TABLE_NAME = 'spotbatch.spotbatchjobparm'
SPOT_MASTER_QUEUE_NAME = 'spotbatch_spotmaster'
SPOT_REQUEST_QUEUE_NAME = 'spotbatch_spotrequest'


MICROSVC_MASTER_CLASSNAME_SpotMasterMessageSubmitBatch = 'SpotMasterMessageSubmitBatch'
MICROSVC_MASTER_CLASSNAME_SpotMasterMessageCheckStatus = 'SpotMasterMessageCheckStatus'
MICROSVC_MASTER_CLASSNAME_SpotMasterMessageIncrSuccessCnt = 'SpotMasterMessageIncrSuccessCnt'
MICROSVC_MASTER_CLASSNAME_SpotMasterMessageResubmitFailedRequest = 'SpotMasterMessageResubmitFailedRequest'
    
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageCheckStatus = 'SpotRequestMessageCheckStatus'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceBatchProcessComplete = 'SpotRequestMessageInstanceBatchProcessComplete'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceBatchProcessStartException = 'SpotRequestMessageInstanceBatchProcessStartException'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceHeartbeat = 'SpotRequestMessageInstanceHeartbeat'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceHeartbeatDaemonStarted = 'SpotRequestMessageInstanceHeartbeatDaemonStarted'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstancePendingTerminationDetected = 'SpotRequestMessageInstancePendingTerminationDetected'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstancePendingTerminationException = 'SpotRequestMessageInstancePendingTerminationException'
MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageSpotRequestInitiated = 'SpotRequestMessageSpotRequestInitiated'

ROLE_NAME_PREFIX = 'spotrole_'
POLICY_NAME_PREFIX = 'SpotPolicy-'

NO_SPOT_INSTANCES_AVAILABLE_RECHECK_MINUTES = 5

DEFAULT_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": "*",
      "Resource": "*"
    }
  ]
}
"""