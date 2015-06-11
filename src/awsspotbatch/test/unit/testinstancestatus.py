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
Unit test - display status of list of instance ids'
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import awsext.ec2
import boto.ec2



def main():     
    """ """
    
    import logging.config
    logging.config.fileConfig( '../../../../config/consoleonly.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting' )
        ec2_conn = awsext.ec2.connect_to_region('us-east-1', profile_name='ipc-training' )

        instance_ids = ['i-6f466493' ]
        
        try:
            instance = ec2_conn.get_only_instances(instance_ids=instance_ids)[0]
            ip_address = instance.ip_address
            print str(ip_address)
        except boto.exception.EC2ResponseError as e:
            if e.code == 'InvalidInstanceID.NotFound':
                # TODO: looks like spot instance was terminated between the time it was allocated and initialized
                pass
            else: 
                # TODO: raise an exception
                pass
        
        for instance_id in instance_ids:
            try:
                state_name, status = ec2_conn.get_instance_state_name_and_status( instance_id )
                print instance_id + ' ' + state_name + ' ' + status
            except StandardError as e:
                logger.error( 'Exception on ' + instance_id + ', ' + str(e) )

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()