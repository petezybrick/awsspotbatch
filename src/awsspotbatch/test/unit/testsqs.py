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
Unit test - display SQS ARN for various queues
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import awsext.iam
import awsext.sqs
import awsspotbatch.common.const
    
def main():
    """ """
    sqs_conn = awsext.sqs.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    queue = sqs_conn.get_queue( awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME )
    print queue.arn
      
if __name__ == "__main__":
    main()
