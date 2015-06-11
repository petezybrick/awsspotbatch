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
Bootstrap program - runs commands from a file to initialize the environment on an EC2 instance
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import subprocess
import os
import logging


def main():     
    """ """
    if( len(sys.argv) < 2 ):
        print 'Invalid format, execution cancelled'
        print 'Correct format: python awsspotbatch.clientbootstrap <cmdfile.txt>'
        sys.exit(8)
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting, cmd_file=' + sys.argv[1] )
        with open(sys.argv[01]) as f:
            cmds = f.readlines()
            
        for cmd in cmds:
            cmd = cmd.strip()
            if len(cmd) == 0: continue
            cmd_name_args = cmd.split( ' ' )
            logger.info('Cmd: ' + str(cmd_name_args))
            if cmd_name_args[0].lower() != 'cd':
                child_process = subprocess.Popen( cmd_name_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
                std_out, std_err = child_process.communicate( )
                returncode = child_process.returncode
            # CD not supported by emulated terminal in Python
            else: 
                if cmd_name_args[1] != '~': cd_path = os.path.abspath(cmd_name_args[1])
                else: cd_path = os.path.abspath(os.path.expanduser('~')) 
                os.chdir(cd_path)    
                std_out = ''
                std_err = ''
                returncode = 0

            logger.info('returncode=' + str(returncode) + ', std_out=' + std_out + ', std_err=' + std_err )
            if returncode > 2: sys.exit( returncode )

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()