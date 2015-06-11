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
Unit test - get STS keys/token and pass to Redshift to unload to S3
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import psycopg2
import subprocess
import json


def find_ec2_instance_credentials_by_role( ):
    """ """
    dict_creds = {}
    cmd = 'curl -s http://169.254.169.254/latest/meta-data/iam/info'
    cmd_name_args = cmd.strip().split( ' ' )
    child_process = subprocess.Popen( cmd_name_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    std_out, std_err = child_process.communicate( )
    returncode = child_process.returncode    
    if returncode == 0 and len(std_out) > 0:
        dict_info = json.loads( std_out )
        arn = dict_info[ 'InstanceProfileArn' ]
        role_name = arn[ (arn.rindex('/')+1): ]
        print 'role_name: ' + role_name
        cmd = 'curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role_name
        cmd_name_args = cmd.strip().split( ' ' )
        child_process = subprocess.Popen( cmd_name_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        std_out, std_err = child_process.communicate( )
        returncode = child_process.returncode
        if returncode == 0 and len(std_out) > 0: dict_creds = json.loads( std_out )
    return dict_creds
    
def main():
    """ """
    profile_name = None
    bucket_staging_name_prefix = 'ipc-datadepot.staging/12345'
    temp_token_creds = None
    if profile_name == None:
        dict_creds = find_ec2_instance_credentials_by_role( )
        temp_token_creds = ''.join( ['aws_access_key_id=', dict_creds['AccessKeyId'], 
                                ';aws_secret_access_key=', dict_creds['SecretAccessKey'], 
                                ';token=', dict_creds['Token'] ])
#     else: 
#         duration_seconds = self.extract_job_parm_item.unload_duration_minutes * 60        
#         sts_conn = boto.sts.connect_to_region( self.extract_job_parm_item.region_name, profile_name=self.extract_job_parm_item.profile_name )
#         federation_token = sts_conn.get_federation_token( 'extract_user', duration=duration_seconds, policy=json.dumps(bucket_staging_policy))
#         temp_token_creds = ''.join( ['aws_access_key_id=', federation_token.credentials.access_key,
#                                     ';aws_secret_access_key=', federation_token.credentials.secret_key,
#                                     ';token=', federation_token.credentials.session_token] )
    
    dsn = "dbname=dbbigdata user=ipcdev password=Ipcdev11 port=5439 host=sitecatdemo-instance.cwti5kwectza.us-east-1.redshift.amazonaws.com"
    query = 'select element_type, element_desc from tbref_element;'
    
    conn = None
    cur = None
    try:
        conn = psycopg2.connect( dsn )
        #with psycopg2.connect( dsn ) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        bucket_path = 's3://' + bucket_staging_name_prefix + '/data/tbref_element/tbref_element_'
        cmd_unload_no_creds = """UNLOAD ('""" + query + """') TO '""" + \
            bucket_path + """' WITH CREDENTIALS AS '$credentials$' ALLOWOVERWRITE DELIMITER '|' GZIP;"""
        cmd_unload_with_creds = cmd_unload_no_creds.replace( '$credentials$', temp_token_creds)
        try:
            cur.execute( cmd_unload_with_creds )
        except StandardError as e:
            print 'Exception executing UNLOAD: ' + cmd_unload_no_creds
            raise e
    except StandardError as e:
        print str(e)
        print traceback.format_exc()
    finally:
        if cur != None: cur.close()
        if conn != None: conn.close()
      
if __name__ == "__main__":
    main()
    
