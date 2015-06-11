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
Deploy latest code, remotely install and restart services
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import logging
import awsext.ec2
from awsext.ec2.remote import RemoteRunRequest, RemoteRunThread
from awsspotbatch.deploy.deployparmitem import DeployParmItem


def full_deploy():
    """Deploy latest code, remotely install and restart services """
    region_name = sys.argv[1]
    profile_name = sys.argv[2]
    deploy_parm_item = DeployParmItem( sys.argv[3] )
    
    remote_run_requests = []
    # awsext python project
    remote_run_requests.append( RemoteRunRequest(from_file=deploy_parm_item.awsext_zip_local_path_name_ext, 
                                                 to_file=deploy_parm_item.awsext_zip_remote_path_name_ext ))

    # awsspotbatch python project
    remote_run_requests.append( RemoteRunRequest(from_file=deploy_parm_item.awsspotbatch_zip_local_path_name_ext, 
                                                 to_file=deploy_parm_item.awsspotbatch_zip_remote_path_name_ext ))
    
    # File of commands
    remote_run_requests.append( RemoteRunRequest(from_file=deploy_parm_item.client_cmds_local_path_name_ext, 
                                                 to_file=deploy_parm_item.client_cmds_remote_path_name_ext ))
    # Bootstrap client program
    remote_run_requests.append( RemoteRunRequest(from_file=deploy_parm_item.client_bootstrap_local_path_name_ext, 
                                                 to_file=deploy_parm_item.client_bootstrap_remote_path_name_ext,
                                                 cmd_line=deploy_parm_item.remote_cmd ))

    remote_run_threads = []
    thread_num = 0
    ec2_conn = awsext.ec2.connect_to_region( region_name, profile_name=profile_name)
    reservations = ec2_conn.get_all_instances( instance_ids=deploy_parm_item.instance_ids )
    for reservation in reservations:
        for instance in reservation.instances:
            remote_run_threads.append( RemoteRunThread( thread_num, instance.ip_address, timeout=60, 
                                                        username='ubuntu', 
                                                        key_filename=deploy_parm_item.key_path_name_ext, 
                                                        remote_run_requests=remote_run_requests  ) )
            logger.info('Processing instance: ' + instance.id  + ", ip_address=" + instance.ip_address )
    
    for remote_run_thread in remote_run_threads: remote_run_thread.start()
    for remote_run_thread in remote_run_threads: remote_run_thread.join()
    logger.info(">>> Results <<<")
    for remote_run_thread in remote_run_threads: 
        logger.info("\tThreadNumber: " + str(remote_run_thread.thread_num) )
        for remote_run_response in remote_run_thread.remote_run_responses:
            logger.info('\t\tReturnCode=' + str(remote_run_response.returncode) +  
                        ', std_out=' + remote_run_response.std_out +
                        ', std_err=' + remote_run_response.std_err +
                        ', cmd_line=' + remote_run_response.cmd_line )


if __name__ == '__main__':
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(thread)d] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)

    full_deploy()