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
Unit test - log message, sleep, log message.  
Also used to verify spot instances, this is a simple test where the sleep time can be set
to multiple hours to test constraints and AWS-initiated spot instance terminations
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import time
import logging

    
def main():     
    """ """
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)
    sleep_mins = 60
    if len(sys.argv) == 2: sleep_mins = int(sys.argv[1])
    logger.info( 'Before sleeping for ' + str(sleep_mins) + ' minutes' )
    time.sleep( sleep_mins*60 )
    logger.info( 'After sleep')
      
if __name__ == "__main__":
    main()