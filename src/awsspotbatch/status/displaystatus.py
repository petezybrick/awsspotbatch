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
Hack to create html file displaying the status of batch jobs
TODO: replace with a simple servlet under Tomcat to generate the html
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import time
import traceback
import logging
import datetime
import operator
import boto.dynamodb2
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.tabledef import TableSpotMaster, TableSpotRequest

logger = logging.getLogger(__name__)

HTML_HEADER = """
<html>
<head>
<META HTTP-EQUIV="refresh" CONTENT="5; URL=file://$path_name_ext_html_page$">
 <STYLE type="text/css">
  .header {  margin-left: 5px ;  margin-right: 5px ; font-family: 'Arial'; text-align:'left'; font-size:12px ; font-weight:bold ; }
  .data {  margin-left: 5px ;  margin-right: 5px ; font-family: 'Arial'; text-align:'left'; font-size:12px ; height:15px; vertical-align:top;}
  .request_table { margin-left: 15px }
  .master_table {  }
 </STYLE>
</head>
<form>
<font face='Arial' size=2>
<div class="header"><p id="current_date"></p></div>
</font>
<script>
document.getElementById("current_date").innerHTML = Date().toString();
</script> 
"""

HTML_TRAILER = """
</form>
</html>
"""

MASTER_ATTRS = [
    TableSpotMaster.spot_master_uuid,
    TableSpotMaster.ts_last_state_check,
    TableSpotMaster.is_open,
    TableSpotMaster.ts_start,
    TableSpotMaster.ts_end,
    TableSpotMaster.spot_master_state_code,
    TableSpotMaster.spot_master_failure_code,
    TableSpotMaster.is_pending_cleanup,
    TableSpotMaster.region_name,
    TableSpotMaster.profile_name,
    TableSpotMaster.ami_id,
    TableSpotMaster.num_instances,
    TableSpotMaster.instance_type,
    TableSpotMaster.instance_username,
    TableSpotMaster.current_attempt_number,
    TableSpotMaster.num_requests_complete_ok,
    TableSpotMaster.spot_attempts_max,
    TableSpotMaster.sg_id,
    TableSpotMaster.kp_name,
    TableSpotMaster.role_name,
    TableSpotMaster.policy_name, 
    TableSpotMaster.policy_json,
    TableSpotMaster.cheapest_vpc_id,
    TableSpotMaster.cheapest_subnet_id,
    TableSpotMaster.cheapest_region_name,
    TableSpotMaster.cheapest_zone_name,
    TableSpotMaster.cheapest_price, 
    ]

MASTER_ATTR_WIDTHS = {
    TableSpotMaster.spot_master_uuid:'250px',
    TableSpotMaster.ts_last_state_check:'125px',
    TableSpotMaster.is_open:'50px',
    TableSpotMaster.spot_master_state_code:'250px',
    TableSpotMaster.spot_master_failure_code:'250px',
    TableSpotMaster.is_pending_cleanup:'50px',
    TableSpotMaster.region_name:'125px',
    TableSpotMaster.profile_name:'125px',
    TableSpotMaster.ami_id:'125px',
    TableSpotMaster.num_instances:'125px',
    TableSpotMaster.instance_type:'125px',
    TableSpotMaster.instance_username:'125px',
    TableSpotMaster.current_attempt_number:'250px',
    TableSpotMaster.ts_start:'125px',
    TableSpotMaster.ts_end:'125px',
    TableSpotMaster.num_requests_complete_ok:'250px',
    TableSpotMaster.spot_attempts_max:'250px',
    TableSpotMaster.sg_id:'250px',
    TableSpotMaster.kp_name:'250px',
    TableSpotMaster.role_name:'250px',
    TableSpotMaster.policy_name:'250px', 
    TableSpotMaster.policy_json:'250px',
    TableSpotMaster.cheapest_vpc_id:'125px',
    TableSpotMaster.cheapest_subnet_id:'125px',
    TableSpotMaster.cheapest_region_name:'125px',
    TableSpotMaster.cheapest_zone_name:'125px',
    TableSpotMaster.cheapest_price:'125px',
    }

REQUEST_ATTRS = [
    TableSpotRequest.spot_request_uuid,
    TableSpotRequest.is_open,
    TableSpotRequest.spot_request_state_code,
    TableSpotRequest.ts_heartbeat,
    TableSpotRequest.spot_request_id,
    TableSpotRequest.ts_last_state_check,
    TableSpotRequest.attempt_number,
    TableSpotRequest.spot_price,
    TableSpotRequest.spot_request_failure_code,
    TableSpotRequest.instance_id,
    TableSpotRequest.instance_public_ip_address,
    TableSpotRequest.instance_username,
    TableSpotRequest.ts_start,
    TableSpotRequest.ts_end,
    TableSpotRequest.ts_heartbeat_daemon_started,
    TableSpotRequest.ts_rejected,
    TableSpotRequest.ts_granted,
    TableSpotRequest.ts_terminated_request_recvd,
    TableSpotRequest.ts_terminated_instance,
    TableSpotRequest.ts_cancel_recvd,
    TableSpotRequest.ts_cancel_complete,
    TableSpotRequest.ts_pending_termination_detected,
    TableSpotRequest.ts_pending_termination_detected,
    TableSpotRequest.termination_time_exception,
    TableSpotRequest.ts_cmd_complete,
    TableSpotRequest.cmd_returncode,
    TableSpotRequest.cmd_std_out,
    TableSpotRequest.cmd_std_err,
    TableSpotRequest.constraint_code,
    TableSpotRequest.cmd_exception_message,
    TableSpotRequest.cmd_exception_traceback, 
    ]

REQUEST_ATTR_WIDTHS = {
    TableSpotRequest.spot_request_uuid:'250px',
    TableSpotRequest.is_open:'50px',
    TableSpotRequest.spot_request_state_code:'200px',
    TableSpotRequest.ts_heartbeat:'125px',
    TableSpotRequest.spot_request_id:'125px',
    TableSpotRequest.ts_last_state_check:'125px',
    TableSpotRequest.attempt_number:'100px',
    TableSpotRequest.spot_price:'100px',
    TableSpotRequest.spot_request_failure_code:'200px',
    TableSpotRequest.instance_id:'100px',
    TableSpotRequest.instance_public_ip_address:'100px',
    TableSpotRequest.instance_username:'100px',
    TableSpotRequest.ts_start:'125px',
    TableSpotRequest.ts_end:'125px',
    TableSpotRequest.ts_heartbeat_daemon_started:'125px',
    TableSpotRequest.ts_rejected:'125px',
    TableSpotRequest.ts_granted:'125px',
    TableSpotRequest.ts_terminated_request_recvd:'125px',
    TableSpotRequest.ts_terminated_instance:'125px',
    TableSpotRequest.ts_cancel_recvd:'125px',
    TableSpotRequest.ts_cancel_complete:'125px',
    TableSpotRequest.ts_pending_termination_detected:'125px',
    TableSpotRequest.ts_pending_termination_detected:'125px',
    TableSpotRequest.termination_time_exception:'125px',
    TableSpotRequest.ts_cmd_complete:'125px',
    TableSpotRequest.cmd_returncode:'125px',
    TableSpotRequest.cmd_std_out:'400px',
    TableSpotRequest.cmd_std_err:'400px',
    TableSpotRequest.constraint_code:'200px',
    TableSpotRequest.cmd_exception_message:'400px',
    TableSpotRequest.cmd_exception_traceback:'400px', 
    }

class SpotRequestStatusItem(object):
    """ """
                                
    def __init__( self, spot_request ):
        """

        :param spot_request: 

        """
        self.spot_request = spot_request
        self.ts_start = spot_request[ TableSpotRequest.ts_start]


class SpotMasterStatusItem(object):
    """ """
                                
    def __init__( self, spot_master=None, spot_requests=None ):
        """

        :param spot_master:  (Default value = None)
        :param spot_requests:  (Default value = None)

        """
        self.spot_master = spot_master
        self.ts_start = spot_master[ TableSpotMaster.ts_start]
        self.spot_request_status_items = []
        for spot_request in spot_requests:
            self.spot_request_status_items.append( SpotRequestStatusItem(spot_request) )
        self.spot_request_status_items.sort( key=operator.attrgetter('ts_start'))
        


class SpotOpenStatus(object):
    """ """
                                
    def __init__( self, region_name=None, profile_name=None ):
        """

        :param region_name:  (Default value = None)
        :param profile_name:  (Default value = None)

        """
        self.region_name = region_name
        self.profile_name = profile_name

        
    def find_all_open_closed(self):
        """ """
        spot_master_status_items_open = []
        spot_master_status_items_closed = []
        dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
        
        spot_master_table = Table( awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, connection=dynamodb_conn ) 
        spot_request_table = Table( awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, connection=dynamodb_conn )
        
        spot_master_status_items = spot_master_table.query_2( is_open__eq=1, index='IsOpen' )
        for spot_master_item in spot_master_status_items:
            spot_master_uuid = spot_master_item[TableSpotMaster.spot_master_uuid]
            spot_request_items = spot_request_table.query_2( spot_master_uuid__eq=spot_master_uuid, index='MasterUuid' )
            spot_master_status_items_open.append( SpotMasterStatusItem( spot_master_item, spot_request_items ))
        spot_master_status_items_open.sort( key=operator.attrgetter('ts_start'))

        
        spot_master_status_items = spot_master_table.query_2( is_open__eq=0, index='IsOpen' )
        for spot_master_item in spot_master_status_items:
            spot_master_uuid = spot_master_item[TableSpotMaster.spot_master_uuid]
            spot_request_items = spot_request_table.query_2( spot_master_uuid__eq=spot_master_uuid, index='MasterUuid' )
            spot_master_status_items_closed.append( SpotMasterStatusItem( spot_master_item, spot_request_items ))
        spot_master_status_items_closed.sort( key=operator.attrgetter('ts_start'))
        
        return spot_master_status_items_open, spot_master_status_items_closed

        
    def create_status_html_page(self, spot_master_status_items_open, spot_master_status_items_closed, path_name_ext_html_page ):
        """

        :param spot_master_status_items_open: 
        :param spot_master_status_items_closed: 
        :param path_name_ext_html_page: 

        """

        out_rows = []
        out_rows.append( HTML_HEADER.replace("$path_name_ext_html_page$", path_name_ext_html_page ) )
        
        out_rows.append( '<div class="header">In Progress</div>' )
        self.write_html_rows( out_rows, spot_master_status_items_open )
        
        out_rows.append( '<div class="header" style="height:20px"></div>' )
            
        out_rows.append( '<div class="header">Completed</div>' )
        self.write_html_rows( out_rows, spot_master_status_items_closed )
            
        out_rows.append( HTML_TRAILER )
        
        with open(path_name_ext_html_page, "w") as html_file:
            html_file.writelines( out_rows )
            
            
    def write_html_rows(self, out_rows, spot_master_status_items ):
        """

        :param out_rows: 
        :param spot_master_status_items: 

        """
        is_first_row = True
        for spot_master_status_item in spot_master_status_items:
            if not is_first_row: out_rows.append('<br>')
            else: is_first_row = False
            
            # Master Header
            out_rows.append('<table cellspacing="2"><tr>')
            for master_attr in MASTER_ATTRS:
                out_rows.append('<td><div class="header">' + master_attr + '</div></td>')
            out_rows.append('</tr>')
            
            # Master Data
            out_rows.append('<tr>')
            for master_attr in MASTER_ATTRS:
                value, title = self.format_value( master_attr, spot_master_status_item.spot_master[ master_attr ] ) 
                out_rows.append('<td title="' + title + '"><div class="data" style="width:' + MASTER_ATTR_WIDTHS[master_attr] + '">' + value + '</div></td>')
            out_rows.append('</tr></table></font>')
            
            # Request Header
            out_rows.append('<div class="request_table"><table><tr>')
            for request_attr in REQUEST_ATTRS:
                out_rows.append('<td><div class="header">' + request_attr + '</div></td>')
            out_rows.append('</tr>')
            
            # Request Data's
            for spot_request_status_item in spot_master_status_item.spot_request_status_items:
                out_rows.append('<tr>')
                for request_attr in REQUEST_ATTRS:
                    value, title = self.format_value( request_attr, spot_request_status_item.spot_request[ request_attr ] )
                    out_rows.append('<td title="' + title + '"><div class="data" style="width:' + REQUEST_ATTR_WIDTHS[request_attr] + '">' + value + '</div></td>')
                out_rows.append('</tr>')
            out_rows.append('</table></div>')

        
    def format_value(self, attr_name, value_in ):
        """

        :param attr_name: 
        :param value_in: 

        """
        title = ''
        if value_in == None: return '', ''
        if attr_name.startswith( 'ts_'):
            try:
                value_out = datetime.datetime.fromtimestamp( value_in ).strftime("%Y-%m-%d %H:%M:%S")
            except StandardError as e:
                # logger.warning("Exception formatting timestamp for " + attr_name + ", value_in=" + str(value_in) + ", exception=" + str(e))
                value_out = str(value_in)
        elif attr_name.startswith( 'is_'):
            if value_in == 1: value_out = 'True'
            else: value_out = 'False'
        else: value_out = str( value_in )
        if len(value_out) > 50:
            title = value_out
            value_out = value_out[0:50] + '...'
        return value_out, title

    
def main():     
    """ """
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    
    try:
        logger.info( 'Starting' )
        spot_open_status = SpotOpenStatus()
        while True:
            spot_master_status_items_open, spot_master_status_items_closed = spot_open_status.find_all_open_closed()
            spot_open_status.create_status_html_page( spot_master_status_items_open, spot_master_status_items_closed, sys.argv[1] )
            time.sleep(15)       
                                                       
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()