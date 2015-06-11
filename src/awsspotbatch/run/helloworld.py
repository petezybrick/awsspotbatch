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
Simple program to assist in troubleshooting.  If this won't run on spot instance, then nothing will
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import time
import logging.config

def main():
    """ """
    logging.config.fileConfig( sys.argv[1], disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    while True:
        logger.info( 'Watson, come here. I need you.' )
        time.sleep(10)
    
    
if __name__ == '__main__':
    main()
    