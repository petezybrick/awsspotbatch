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
Unit test - serialize/deserialize key from DynamoDB table, SSH into instance based on key
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import time
import paramiko
import awsext.ec2
import boto.dynamodb2
from cStringIO import StringIO
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
import base64

kp_enc_key = 'vmweu9v8hj23vnzxcv9015jklxcm,xv&swjklasdfkl3#0-1'

def encode(key, clear):
    """

    :param key: 
    :param clear: 

    """
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc))

def decode(key, enc):
    """

    :param key: 
    :param enc: 

    """
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)


def create_store_key():
    """ """
    ec2_conn = awsext.ec2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    unique_key_pair = ec2_conn.create_unique_key_pair_sync( 'pztestkp_' )
    print unique_key_pair.name
    print unique_key_pair.material
    
    enc = encode( kp_enc_key, unique_key_pair.material )
    dec = decode( kp_enc_key, enc )
    print len(enc)
    
    if unique_key_pair.material == dec: print 'OK!'
    
    spot_master_uuid ='4d704b2a-e31c-11e4-bbe6-101f74edff46'
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_master_table = Table(awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, connection=dynamodb_conn ) 
    spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )
    spot_master_item[ 'kp_name' ] = unique_key_pair.name
    spot_master_item[ 'kp_material' ] = enc
    spot_master_item.partial_save()


def retrieve_key():
    """ """
    spot_master_uuid ='17391b76-eb6a-11e4-970f-101f74edff46'
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_master_table = Table(awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, connection=dynamodb_conn ) 
    spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )    
    
    print spot_master_item[ 'kp_name' ]
    print spot_master_item[ 'kp_material' ]
    kp_material_dec =  decode( kp_enc_key, str(spot_master_item[ 'kp_material' ]) )
    print kp_material_dec


def ssh_to_instance():
    """ """
    spot_master_uuid ='4d704b2a-e31c-11e4-bbe6-101f74edff46'
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_master_table = Table(awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, connection=dynamodb_conn ) 
    spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )    
    
    kp_material_dec = decode( kp_enc_key, str(spot_master_item[ 'kp_material' ]) )
    key_file_obj = StringIO( kp_material_dec )
    pkey = paramiko.RSAKey.from_private_key( key_file_obj )
    ip_address = '52.6.69.206'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect( ip_address, timeout=10, username='ec2-user', key_filename='/home/pete.zybrick/.pzkeys/pztestkp_142910758351.pem' )
    ssh.connect( ip_address, timeout=10, username='ec2-user', pkey=pkey )
    
    sftp_client = ssh.open_sftp()
    sftp_client.put( '/home/pete.zybrick/Development/Workspaces/Python/PythonFirst/src/root/nested/helloworld.py', 'helloworld.py' )
    sftp_client.close()
    
    chan = ssh._transport.open_session()   
    chan.exec_command('python helloworld.py')
    print( chan.exit_status_ready() )
    
    # note that out/err doesn't have inter-stream ordering locked down.
    stdout = chan.makefile('rb', -1)
    stderr = chan.makefile_stderr('rb', -1)
    buf_std_out = ''.join(stdout.readlines())
    # print( chan.exit_status_ready() )
    buf_std_err = ''.join(stderr.readlines())
    remote_exit_status = chan.recv_exit_status()
    ssh.close()
    
    print remote_exit_status
    print buf_std_out
    print buf_std_err
    
    
def main():     
    """ """
    # create_store_key()
    retrieve_key()
    # ssh_to_instance()
      
if __name__ == "__main__":
    main()