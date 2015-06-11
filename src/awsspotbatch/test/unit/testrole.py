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
Unit test - various role actions, typically to access roles automatically generated by the Master
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import awsext.iam
import awsspotbatch.common.const
import boto.dynamodb2
from boto.dynamodb2.table import Table
from awsspotbatch.common.util import decode, kp_enc_key, encode
from awsspotbatch.common.tabledef import TableSpotRSAKey
   
   
def role_exists():
    """ """
    try:
        iam_conn = awsext.iam.connect_to_region( 'us-east-1', profile_name='spotbatchmgr.dev' )
        is_role_exists = iam_conn.is_role_exists( "spotrole_143204100317x" )
        print is_role_exists
    except StandardError as e:
        print e
   
   
def delete_role():
    """ """
    try:
        iam_conn = awsext.iam.connect_to_region( 'us-east-1', profile_name='spotbatchmgr.dev' )
        iam_conn.delete_role( "spotrole_143268045302")
    except StandardError as e:
        print e

def delete_instance_profiles():
    """ """
    iam_conn = awsext.iam.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    # cleanup instance profiles
    list_instance_profiles_response = iam_conn.list_instance_profiles( )
    for instance_profile in list_instance_profiles_response.instance_profiles:
        if instance_profile.instance_profile_name.startswith('spotrole_'):
            if instance_profile.roles.has_key('member'):
                member = instance_profile.roles['member']
                role_name = member['role_name']
                print role_name
                iam_conn.remove_role_from_instance_profile( instance_profile.instance_profile_name, role_name )

            print instance_profile.instance_profile_name
            try:
                iam_conn.delete_instance_profile( instance_profile.instance_profile_name )
            except StandardError as e:
                print e


def display_keypair():
    """ """
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_rsa_key_table = Table( awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, connection=dynamodb_conn )
    rsa_key_item = spot_rsa_key_table.get_item( spot_master_uuid='a1ebbe4c-ea8c-11e4-8b6e-101f74edff46' )
    kp_material_dec = decode( kp_enc_key, str( rsa_key_item[ TableSpotRSAKey.rsa_key_encoded ]) )
    print kp_material_dec


def encode_private_key():
    """ """
    with open( '/home/pete.zybrick/.pzkeys/spotkp_142972682114.pem') as f:
        raw = f.read()
    print encode( kp_enc_key, str(raw) )

    
    
def main():
    """ """
    # display_keypair()
    role_exists()

      
if __name__ == "__main__":
    main()
