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
Unit test - read RSA Key from table, decode, display.  Helpful for SSH'ing into spot instance where the script failed
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import awsspotbatch.common.const
import boto.dynamodb2
from boto.dynamodb2.table import Table
from awsspotbatch.common.util import decode, kp_enc_key, encode
from awsspotbatch.common.tabledef import TableSpotRSAKey
   


def display_keypair():
    """ """
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_rsa_key_table = Table( awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, connection=dynamodb_conn )
    rsa_key_item = spot_rsa_key_table.get_item( spot_master_uuid='4d70b3da-f5af-11e4-b866-101f74edff46' )
    kp_material_dec = decode( kp_enc_key, str( rsa_key_item[ TableSpotRSAKey.rsa_key_encoded ]) )
    print kp_material_dec


def encode_private_key():
    """ """
    with open( '/home/pete.zybrick/.pzkeys/spotkp_142972682114.pem') as f:
        raw = f.read()
    print encode( kp_enc_key, str(raw) )

    
    
def main():
    """ """
    display_keypair()

      
if __name__ == "__main__":
    main()
