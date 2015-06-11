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
Common methods for awsspotbatch.microsvc.request
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import threading
import time
import paramiko
import json
import boto.dynamodb2
import awsext.ec2
import awsspotbatch.common.exception
from cStringIO import StringIO
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import TableSpotRequest, SpotRequestStateCode,\
    TableSpotRSAKey, TableSpotBatchJobParm
from awsspotbatch.common.util import decode, kp_enc_key, create_named_in_memory_temp_file
from awsext.ec2.connection import AwsExtEC2Connection
from awsspotbatch.microsvc.request.spotrequestmessagebase import SpotRequestMessageBase
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem
from awsspotbatch.microsvc import get_batch_job_parm_item


import logging
logger = logging.getLogger(__name__)
          

def spot_request_row_partial_save( spot_request_table_name, spot_request_item, 
                                   dict_keys_values, region_name='us-east-1', profile_name=None ):
    """ Save only the changed values as passed in the dict_keys_values dictionary

    :param spot_request_table_name: 
    :param spot_request_item: 
    :param dict_keys_values: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    :raise awsspotbatch.common.exception.DynamoDbPartialSaveError: could not save within max attempts 

    """
    for key,value in dict_keys_values.iteritems(): spot_request_item[ key ] = value
    try:
        partial_save_result = spot_request_item.partial_save()
        if partial_save_result: return      # success on first partial save attempt
    except StandardError:
        pass
    
    # First partial save failed, try a few more times
    spot_request_uuid =  spot_request_item[ TableSpotRequest.spot_request_uuid ]
    max_attempts = 60
    num_attempts = 0
    while True:
        dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
        spot_request_table = Table( spot_request_table_name, connection=dynamodb_conn ) 
        spot_request_item = spot_request_table.get_item( spot_request_uuid=spot_request_uuid )
        for key,value in dict_keys_values.iteritems(): spot_request_item[ key ] = value
        try:
            partial_save_result = spot_request_item.partial_save()
            # partial_save_result = request_item.partial_save()
            if partial_save_result: break
        except StandardError:
            pass            
        num_attempts += 1
        if num_attempts == max_attempts: 
            raise awsspotbatch.common.exception.DynamoDbPartialSaveError('Exceeded partial save attempts on request table, spot_request_uuid=' + spot_request_uuid, spot_request_table_name )
        time.sleep(1)


# def get_spot_batch_job_parm_item( spot_batch_job_parm_table_name, spot_master_uuid, region_name='us-east-1', profile_name=None  ):
#     # TODO: put in loop to retry
#     dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
#     spot_batch_job_parm_table = Table( spot_batch_job_parm_table_name, connection=dynamodb_conn ) 
#     spot_batch_job_parm_item = spot_batch_job_parm_table.get_item( spot_master_uuid=spot_master_uuid )
#     return spot_batch_job_parm_item

        
def get_spot_request_item( spot_request_table_name, spot_request_uuid, region_name='us-east-1', profile_name=None  ):
    """ Get SpotRequestItem based on spot_request_uuid

    :param spot_request_table_name: 
    :param spot_request_uuid: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    :return: SpotRequestItem or None if not found

    """
    # TODO: put in loop to retry
    dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
    spot_request_table = Table( spot_request_table_name, connection=dynamodb_conn ) 
    spot_request_item = None
    try:
        spot_request_item = spot_request_table.get_item( spot_request_uuid=spot_request_uuid )
    except boto.dynamodb2.exceptions.ItemNotFound as e:
        pass
    return spot_request_item

        
def launch_remote_client( spot_batch_job_parm_table_name, spot_rsa_key_table_name, spot_request_item, region_name='us-east-1', profile_name=None ):
    """ SSH into remote client, SCP files to client, run script on client, return results

    :param spot_batch_job_parm_table_name: 
    :param spot_rsa_key_table_name: 
    :param spot_request_item: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)
    return: client_bootstrap_service_cmds_results, client_bootstrap_user_cmds_results

    """
    spot_batch_job_parm_item = get_batch_job_parm_item( spot_request_item[ TableSpotRequest.spot_master_uuid ], spot_batch_job_parm_table_name,  
                                                             region_name=region_name, profile_name=profile_name )
    batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_batch_job_parm_item[ TableSpotBatchJobParm.raw_batch_job_parm_item ] )

    filename_client_parm_json = 'clientparm.json'
    filename_bootstrap_service_cmds = 'bootstrap_service_cmds'
    filename_bootstrap_user_cmds = 'bootstrap_user_cmds'
    cmd_client_launch = 'python -m awsspotbatch.client.clientlaunch ' + filename_client_parm_json + ' &'
    client_parm_json_string = create_client_parm_json_string( spot_request_item, batch_job_parm_item )
    # Get the RSA key, connect to remote instance and launch remote script
    rsa_key_item = get_rsa_key_item( spot_rsa_key_table_name, spot_request_item[ TableSpotRequest.spot_master_uuid ], region_name=region_name, profile_name=profile_name )
    kp_material_dec = decode( kp_enc_key, str( rsa_key_item[ TableSpotRSAKey.rsa_key_encoded ]) )
    key_file_obj = StringIO( kp_material_dec )
    pkey = paramiko.RSAKey.from_private_key( key_file_obj )
    
    instance_public_ip_address =  spot_request_item[ TableSpotRequest.instance_public_ip_address ]
    instance_username = spot_request_item[ TableSpotRequest.instance_username ]
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect( instance_public_ip_address, timeout=10, username=instance_username, pkey=pkey )
    
    # Bootstrap the system and user command
    client_bootstrap_service_primary_results = run_cmds(ssh, [batch_job_parm_item.client_bootstrap_service_primary] )
    if client_bootstrap_service_primary_results[0]['remote_exit_status'] > 2:
        logger.error( fmt_request_item_msg_hdr( spot_request_item ) + 'SEVERE ERROR: client_bootstrap_service_primary failed with remote_exit_status=' + 
                        str(client_bootstrap_service_primary_results[0]['remote_exit_status']) + 
                         ', buf_std_out' + str(client_bootstrap_service_primary_results[0]['buf_std_out'])+ 
                         ', buf_std_err' + str(client_bootstrap_service_primary_results[0]['buf_std_err'])  )
    write_cmd_file_to_remote( ssh, batch_job_parm_item.client_bootstrap_service_cmds, filename_bootstrap_service_cmds )
    client_bootstrap_service_cmds_results = run_cmds(ssh, ['python service/clientbootstrap.py ' + filename_bootstrap_service_cmds])
    write_cmd_file_to_remote( ssh, batch_job_parm_item.client_bootstrap_user_cmds, filename_bootstrap_user_cmds )
    client_bootstrap_user_cmds_results = run_cmds(ssh, ['python service/clientbootstrap.py ' + filename_bootstrap_user_cmds])
    
    ########################
    for cmd_result in client_bootstrap_service_cmds_results:
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '   service cmd: ' + cmd_result['cmd'])    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      remote_exit_status: ' + str(cmd_result['remote_exit_status']) )    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      buf_std_out: ' + cmd_result['buf_std_out'] )    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      buf_std_err: ' + cmd_result['buf_std_err'] )    
    for cmd_result in client_bootstrap_user_cmds_results:
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '   user cmd: ' + cmd_result['cmd'])    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      remote_exit_status: ' + str(cmd_result['remote_exit_status']) )    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      buf_std_out: ' + cmd_result['buf_std_out'] )    
        logger.info( fmt_request_item_msg_hdr( spot_request_item ) +  '      buf_std_err: ' + cmd_result['buf_std_err'] )    
    #########################   
    
    # put the parm file out to this instance - will have a different SpotRequestUUID and SpotRequestiID for each instance
    with create_named_in_memory_temp_file( client_parm_json_string ) as temp_parm_file:
        sftp_client = ssh.open_sftp()
        sftp_client.put( temp_parm_file, filename_client_parm_json )
        sftp_client.close()
    
    # write the user job parm item json file to disk - will be the same on every instance
    # user parm file is optional
    if TableSpotBatchJobParm.raw_user_job_parm_item in spot_batch_job_parm_item:
        user_job_parm_item_filename = 'userjobparmitem.json'
        user_job_parm_item_string = spot_batch_job_parm_item[ TableSpotBatchJobParm.raw_user_job_parm_item ]
        with create_named_in_memory_temp_file( user_job_parm_item_string ) as temp_user_job_parm_file:
            sftp_client = ssh.open_sftp()
            sftp_client.put( temp_user_job_parm_file, user_job_parm_item_filename )
            sftp_client.close()    
    
    # Don't wait for the clientlaunch to complete - it's running the users batch job and a monitor thread that sends SQS status msgs 
    chan = ssh._transport.open_session()   
    chan.exec_command( cmd_client_launch )
    
    ssh.close()

    return client_bootstrap_service_cmds_results, client_bootstrap_user_cmds_results


def write_cmd_file_to_remote( ssh, cmds, file_name_cmds ):
    """ Create text file on remote containing list of commands to execute
        SCP directly from memory, since this is a microservice need to assume no disk available 

    :param ssh: existing paramiko SSH connection
    :param cmds: list of commands to execute
    :param file_name_cmds: path/name.ext to save file remotely

    """
    cmds_string = '\n'.join(cmds)
    with create_named_in_memory_temp_file( cmds_string ) as temp_cmd_file:
        sftp_client = ssh.open_sftp()
        sftp_client.put( temp_cmd_file, file_name_cmds )
        sftp_client.close()
   

def run_cmds( ssh, cmds ):
    """ Run a list of commands on remote, 

    :param ssh: SSH connection
    :param cmds: list of commands to execute
    :return: list of dictionaries, where each dictionary contains the command, return code, std_out and std_err for each command 

    """
    cmd_results = []
    for cmd in cmds:
        chan = ssh._transport.open_session()   
        chan.exec_command( cmd )        
        # note that out/err doesn't have inter-stream ordering locked down.
        stdout = chan.makefile('rb', -1)
        stderr = chan.makefile_stderr('rb', -1)
        buf_std_out = ''.join(stdout.readlines())
        buf_std_err = ''.join(stderr.readlines())
        remote_exit_status = chan.recv_exit_status()
        cmd_results.append( {'cmd':cmd, 'remote_exit_status':remote_exit_status, 'buf_std_out':buf_std_out, 'buf_std_err':buf_std_err })
    return cmd_results


def create_client_parm_json_string( spot_request_item, batch_job_parm_item ):
    """ Write the client parm file on the remote, this is used by ClientLaunch to launch the client script

    :param spot_request_item: 
    :param batch_job_parm_item: 

    """
    client_parm_json_dict = {    
        "region_name":batch_job_parm_item.primary_region_name,
        "spot_request_uuid":spot_request_item[TableSpotRequest.spot_request_uuid],
        "spot_master_uuid":spot_request_item[TableSpotRequest.spot_master_uuid],
        "spot_request_id":spot_request_item[TableSpotRequest.spot_request_id],
        "spot_request_queue_name":batch_job_parm_item.spot_request_queue_name,
        "script_name_args":batch_job_parm_item.script_name_args
    }
    return json.dumps( client_parm_json_dict )


def get_rsa_key_item( spot_rsa_key_table_name, spot_master_uuid, region_name='us-east-1', profile_name=None ):
    """ Get the RSA Key item so SSH can use it to connect to remote instances (same key used on all spot instances)

    :param spot_rsa_key_table_name: 
    :param spot_master_uuid: 
    :param region_name:  (Default value = 'us-east-1')
    :param profile_name:  (Default value = None)

    """
    get_attempt_cnt = 0
    get_attempt_max = 10
    while True:
        dynamodb_conn = boto.dynamodb2.connect_to_region( region_name, profile_name=profile_name )
        spot_rsa_key_table = Table( spot_rsa_key_table_name, connection=dynamodb_conn )
        try:
            rsa_key_item = spot_rsa_key_table.get_item( spot_master_uuid=spot_master_uuid )
            return rsa_key_item
        except StandardError as e:
            get_attempt_cnt += 1
            if get_attempt_cnt == get_attempt_max: 
                raise awsspotbatch.common.exception.DynamoDbGetItemMaxAttemptsExceeded('Failed attempt to get item from: ' + spot_rsa_key_table_name + 
                                                         ' for spot_master_uuid: ' + spot_master_uuid + ' due to exception: ' + e.get_message(), spot_rsa_key_table_name )
            time.sleep(6)



def fmt_request_item_msg_hdr( spot_request_item ):
    """ Helper method for formatting log messages, ensures the Request UUID is always in the log message

    :param spot_request_item: 

    """
    return ' spot_request_uuid=' + spot_request_item[TableSpotRequest.spot_request_uuid] + ' '


def fmt_request_uuid_msg_hdr( spot_request_guid ):
    """ Helper method for formatting log messages, ensures the Request UUID is always in the log message

    :param spot_request_guid: 

    """
    return ' spot_request_uuid=' + spot_request_guid + ' '
