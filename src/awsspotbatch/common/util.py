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
Utilities
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import base64
import awsext.sqs
import os
import tempfile
from contextlib import contextmanager

kp_enc_key = 'vmweu9v8hj23vnzxcv9015jklxcm,xv&swjklasdfkl3#0-1'


def encode(key, clear_text):
    """Encode the clear_text using key

    :param key: key for encoding
    :param clear: clear text to be encoded
    :return: encoded clear_text

    """
    enc = []
    for i in range(len(clear_text)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear_text[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc))


def decode(key, enc):
    """Decode the encoded string

    :param key: key for decoding
    :param enc: encoded string
    :return: decoded string

    """
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)

@contextmanager
def create_named_in_memory_temp_file(data):
    """ in memory named temporary file - will work with sftp put of in-memory file to remote physical file
        http://stackoverflow.com/questions/11892623/python-stringio-and-compatibility-with-with-statement-context-manager/11892712#11892712

    :param data: raw data to be written to in-memory temp file

    """
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.write(data)
    temp.close()
    yield temp.name
    os.unlink(temp.name)


def create_microsvc_message_attributes( service_class_name ):
    """Create message attribute to define the target microservice class

    :param service_class_name: 

    """
    return  { "service_class_name": {
                                     "data_type": "String",
                                     "string_value": service_class_name
                                     }
            }


def trimStdOutErrSqsPayload( std_out, std_err ):    
    """SQS payload max length is 256K, so trim down to 250K for the out/err lengths if necessary

    :param std_out: 
    :param std_err: 

    """

    max_payload_len = 250*1024
    if len(std_out) + len(std_err) > max_payload_len:
        if len(std_err) == 0: std_out = std_out[:max_payload_len] + '...'
        elif len(std_out) == 0: std_err = std_err[:max_payload_len] + '...'
        else:
            tot_len = len(std_out) + len(std_err)
            std_out_len_adj = int( ( len(std_out) / float(tot_len) ) * tot_len )
            std_err_len_adj = int( ( len(std_err) / float(tot_len) ) * tot_len )
            std_out = std_out[:std_out_len_adj] + '...'
            std_err = std_err[:std_err_len_adj] + '...'
    return std_out, std_err


def create_policy( batch_job_parm_item ):
    """Create the policy document that will be assigned to the Role used by Spot Instances to 
        1. communicate State via SQS messages
        2. Access S3 bucket containing code/data required for batch execution (i.e. Python zips for awsext and awsspotbatch)
        3. Access S3 bucket containing user data to sync to the Spot instance

    :param batch_job_parm_item: BatchJobParmItem containing additional policy statements
    :return: policy document dictionary

    """
    sqs_conn = awsext.sqs.connect_to_region( batch_job_parm_item.spot_request_queue_region_name, profile_name=batch_job_parm_item.profile_name )
    spot_request_queue = sqs_conn.get_queue( batch_job_parm_item.spot_request_queue_name )
    policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "RequestQueue",
                        "Effect": "Allow",
                        "Action": [
                            "sqs:GetQueueUrl",
                            "sqs:SendMessage",
                            "sqs:SetQueueAttributes"
                        ],
                        "Resource": [
                            spot_request_queue.arn
                        ]
                    },
                    {
                        "Sid": "ServiceBucket",
                        "Effect": "Allow",
                        "Action": [
                            "s3:ListObject",
                            "s3:GetObject"
                        ],
                        "Resource": [
                            "arn:aws:s3:::"+batch_job_parm_item.service_bucket_name+"/*",
                            "arn:aws:s3:::"+batch_job_parm_item.service_bucket_name
                        ]
                    },
                    {
                        "Sid": "UserInputBucket",
                        "Effect": "Allow",
                        "Action": [
                            "s3:ListObject",
                            "s3:GetObject"
                        ],
                        "Resource": [
                            "arn:aws:s3:::"+batch_job_parm_item.user_bucket_name+"/*",
                            "arn:aws:s3:::"+batch_job_parm_item.user_bucket_name
                        ]
                    }
                ]
            }

    if batch_job_parm_item.policy_statements != None and len(batch_job_parm_item.policy_statements) > 0:
        policy["Statement"].extend( batch_job_parm_item.policy_statements )
    return policy

