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
Unit test - empty s3 bucket
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import boto.s3
import traceback

def empty_delete_bucket( bucket_name, region_name, profile_name ):
    """

    :param bucket_name: 
    :param region_name: 
    :param profile_name: 

    """
    s3_conn = boto.s3.connect_to_region( region_name, profile_name=profile_name )
    bucket = s3_conn.get_bucket(bucket_name)
    bucketListResultSet = bucket.list()
    result = bucket.delete_keys([key.name for key in bucketListResultSet])
    #s3_conn.delete_bucket(bucket)
    
    
def main():
    """ """
    try:
        bucket_target_names = [
                               'ipc-datadepot.staging',
                               ]
        for bucket_target_name in bucket_target_names:
            empty_delete_bucket( bucket_target_name, 'us-east-1', 'ipc-training' )

        
    except StandardError as e:
        print str(e)
        print traceback.format_exc()  
        
        
if __name__ == "__main__":
    main()
