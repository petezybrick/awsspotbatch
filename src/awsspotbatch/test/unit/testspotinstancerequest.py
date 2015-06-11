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
Unit test - find cheapest region/az price and submit spot requests
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import datetime
import awsext.ec2
import boto.dynamodb2
from awsext.ec2.spotprice import find_spot_cheapest_prices 
from awsspotbatch.common.batchjobparmitem import BatchJobParmItem
import awsspotbatch.common.const
from boto.dynamodb2.table import Table
from awsspotbatch.common.tabledef import TableSpotMaster, TableSpotBatchJobParm


def calc_bid_price( cheapest_price ):
    """

    :param cheapest_price: 

    """
    # For now, just uplift by 10% to increase probability of not getting terminated
    return cheapest_price /2 #* 1.10
    

def submit_request_spot_instances( spot_master_item, dynamodb_conn, profile_name=None ):
    """

    :param spot_master_item: 
    :param dynamodb_conn: 
    :param profile_name:  (Default value = None)

    """
    spot_instance_requests = None
    bid_price = -1.0
    cheapest_subnet = None
    spot_batch_job_parm_table = Table(awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME, connection=dynamodb_conn ) 
    spot_batch_job_parm_item = spot_batch_job_parm_table.get_item( spot_master_uuid=spot_master_item[TableSpotMaster.spot_master_uuid] )
    
    batch_job_parm_item = BatchJobParmItem( stringParmFile=spot_batch_job_parm_item[TableSpotBatchJobParm.raw_batch_job_parm_item] )
    spot_cheapest_items = find_spot_cheapest_prices( instance_type=batch_job_parm_item.instance_type, 
                                                     product_description='Linux/UNIX', 
                                                     profile_name=profile_name, 
                                                     region_filter=batch_job_parm_item.spot_query_region_names, 
                                                     max_bid=batch_job_parm_item.spot_query_max_bid)
    if len(spot_cheapest_items) > 0:
        spot_cheapest_item = spot_cheapest_items[0]
        spot_query_regions_azs_subnets = batch_job_parm_item.spot_query_regions_azs_subnets
        cheapest_subnet = None
        if spot_cheapest_item.region.name in spot_query_regions_azs_subnets:
            azs_subnets = spot_query_regions_azs_subnets[ spot_cheapest_item.region.name ]
            if spot_cheapest_item.zone.name in azs_subnets:
                cheapest_subnet = azs_subnets[ spot_cheapest_item.zone.name ]
        
        bid_price = calc_bid_price( spot_cheapest_item.price )
        
        valid_until = datetime.datetime.utcnow() + datetime.timedelta( minutes=batch_job_parm_item.spot_request_valid_for_minutes )
        valid_until_iso = valid_until.isoformat()
        ec2_conn = awsext.ec2.connect_to_region( spot_cheapest_item.region.name )
        spot_instance_requests = ec2_conn.request_spot_instances(
            bid_price, 
            image_id=batch_job_parm_item.ami_id, 
            count=batch_job_parm_item.num_instances, 
            type='one-time', 
            dry_run=batch_job_parm_item.dry_run,
            valid_until=valid_until_iso, 
            user_data=None, 
            instance_type=spot_cheapest_item.instance_type,
            placement=spot_cheapest_item.zone.name, 
            instance_profile_name=spot_master_item[ TableSpotMaster.role_name ],
            key_name=spot_master_item[ TableSpotMaster.kp_name ],
            security_group_ids=[ spot_master_item[ TableSpotMaster.sg_id ] ],
            subnet_id=cheapest_subnet,
            availability_zone_group='single',
            )
            
    return spot_instance_requests, bid_price, cheapest_subnet  
    
    
def main():
    """ """
    # This will be created when the job is first submitted
    spot_master_uuid = '908c2d26-e9d3-11e4-9708-101f74edff46'
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    spot_master_table = Table(awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, connection=dynamodb_conn ) 
    spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )

    spot_instance_requests, bid_price, subnet = submit_request_spot_instances( spot_master_item, dynamodb_conn, profile_name='ipc-training' )
    if spot_instance_requests != None:
        for spot_instance_request in spot_instance_requests:
            print str(spot_instance_request)

      
if __name__ == "__main__":
    main()
