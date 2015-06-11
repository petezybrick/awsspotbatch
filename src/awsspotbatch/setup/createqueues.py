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
Create Master and Request SQS Queues - run once during setup - see README
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

from awsspotbatch.setup.queuemgr import QueueMgr


def main():
    """ """
    import logging.config
    logging.config.fileConfig( '../../../config/consoleonly.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.info("Start")
    queue_mgr = QueueMgr( 'us-east-1', profile_name='ipc-training' )
    queue_mgr.create_queues()
    # queue_mgr.send_test_data()
    # queue_mgr.receive_test_data()
    logger.info("End")

      
if __name__ == "__main__":
    main()