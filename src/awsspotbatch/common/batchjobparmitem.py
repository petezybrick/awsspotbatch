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
Contains attributes necessary for running a Batch Job
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import json
from awsext.vpc.connection import InboundRuleItem

def serialize_inbound_rule_items( list_rule_maps ):
    """Convert list of maps of ingress rules to single string TSV/newline

    :param list_rule_maps: 

    """
    serialized_inbound_rule_items = ''
    for rule_map in list_rule_maps:
        ip_protocol='tcp'
        from_port=22
        to_port=None
        cidr_ip='0.0.0.0/0'
        if 'ip_protocol' in rule_map: ip_protocol=rule_map['ip_protocol']
        if 'from_port' in rule_map: from_port=rule_map['from_port']
        if 'to_port' in rule_map: to_port=rule_map['to_port']
        if 'cidr_ip' in rule_map: cidr_ip=rule_map['cidr_ip']
        # this is a hack, but there will be a small number of inbound rules and only processed once, fit the hammer to the nail
        serialized_inbound_rule_items += ip_protocol + '\t' + str(from_port) + '\t' + str(to_port) + '\t' + cidr_ip + '\n' 
    return serialized_inbound_rule_items


def deserialize_inbound_rule_items( serialized_inbound_rule_items ):
    """Convert single string TSV/newline to list of maps of ingress rules 

    :param serialized_inbound_rule_items: 

    """
    inbound_rule_items = []
    rule_items_list = serialized_inbound_rule_items.splitlines()
    for rule_item in rule_items_list:
        tokens = rule_item.split()
        ip_protocol='tcp'
        from_port=22
        to_port=None
        cidr_ip='0.0.0.0/0'
        if tokens[0] != 'None': ip_protocol = tokens[0]
        if tokens[1] != 'None': from_port = int(tokens[1])
        if tokens[2] != 'None': to_port = int(tokens[2])
        if tokens[3] != 'None': cidr_ip = tokens[3]
        
        inbound_rule_items.append( InboundRuleItem(ip_protocol=ip_protocol,
                                                   from_port=from_port, 
                                                   to_port=to_port, 
                                                   cidr_ip=cidr_ip ) )
    return inbound_rule_items


class BatchJobParmItem(object):
    """Contains attributes necessary for running a Batch Job"""

    def __init__(self, pathParmFile=None, stringParmFile=None ):
        """

        :param pathParmFile: path/name.ext of parm file (Default value = None)
        :param stringParmFile: string representation of parm file  (Default value = None)

        """
        if pathParmFile != None: 
            with open( pathParmFile ) as json_data:
                map_parms = json.load(json_data)
                json_data.close()
        elif stringParmFile != None: 
            map_parms = json.loads(stringParmFile)
        else: raise ValueError('pathParmFile or stringParmFile must be passed')

        temp_boolean = map_parms[ 'dry_run' ].lower()
        if 'false' == temp_boolean: self.dry_run = False
        else: self.dry_run = True
        self.script_name_args = map_parms['script_name_args']
        self.ami_id = map_parms['ami_id']
        self.num_instances = map_parms['num_instances']
        self.instance_type = map_parms['instance_type']
        self.instance_username = map_parms['instance_username']
        
        self.primary_region_name = map_parms['primary_region_name']
        if 'profile_name' in map_parms: self.profile_name = map_parms['profile_name']
        else: self.profile_name = None
        if self.profile_name == 'None': self.profile_name = None
        self.region_names = map_parms['region_names']
        self.vpc_ids_by_region_names = map_parms['vpc_ids_by_region_names']
        self.spot_request_valid_for_minutes = int(map_parms['spot_request_valid_for_minutes'])
        self.spot_request_queue_region_name = map_parms['spot_request_queue_region_name']
        self.spot_request_queue_name = map_parms['spot_request_queue_name']
        if 'policy_statements' in map_parms: self.policy_statements = map_parms['policy_statements']
        else: self.policy_statements = None
        self.service_bucket_name = map_parms['service_bucket_name']
        self.user_bucket_name = map_parms['user_bucket_name']
        self.spot_query_max_bid = float(map_parms['spot_query_max_bid'])
        self.spot_query_region_names = map_parms['spot_query_region_names']
        self.spot_query_regions_azs_subnets = map_parms['spot_query_regions_azs_subnets']        
        self.client_bootstrap_service_primary = map_parms['client_bootstrap_service_primary']
        self.client_bootstrap_service_cmds = map_parms['client_bootstrap_service_cmds']
        self.client_bootstrap_user_cmds = map_parms['client_bootstrap_user_cmds']

        self.serialized_inbound_rule_items = serialize_inbound_rule_items( map_parms['inbound_rule_items'] )
