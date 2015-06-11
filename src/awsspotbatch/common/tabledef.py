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
Constant definitions of every attribute for every DynamoDB table, as well as State and Error codes
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

class TableSpotMaster:
    """Master table attributes"""
    spot_master_uuid = 'spot_master_uuid'
    ts_last_state_check = 'ts_last_state_check'
    is_open = 'is_open'
    spot_master_state_code = 'spot_master_state_code'
    spot_master_failure_code = 'spot_master_failure_code'
    is_pending_cleanup = 'is_pending_cleanup'
    region_name = 'region_name'
    profile_name = 'profile_name'
    ami_id = 'ami_id'
    num_instances = 'num_instances'
    instance_type = 'instance_type'
    instance_username = 'instance_username'
    current_attempt_number = 'current_attempt_number'
    ts_start = 'ts_start'
    ts_end = 'ts_end'
    num_requests_complete_ok = 'num_requests_complete_ok'
    spot_attempts_max = 'spot_attempts_max'
    sg_id = 'sg_id'
    kp_name = 'kp_name'
    role_name = 'role_name'
    policy_name = 'policy_name' 
    policy_json = 'policy_json'
    cheapest_vpc_id = 'cheapest_vpc_id'
    cheapest_subnet_id = 'cheapest_subnet_id'
    cheapest_region_name = 'cheapest_region_name'
    cheapest_zone_name = 'cheapest_zone_name'
    cheapest_price = 'cheapest_price'

    
class SpotMasterStateCode:
    """Master state codes """
    batch_submit = 'batch_submit'
    master_resources_in_progress = 'master_resources_in_progress'
    master_role_policy_in_progress = 'master_role_policy_in_progress'
    waiting_for_instances_complete = 'waiting_for_instances_complete'
    waiting_for_instances_terminated = 'waiting_for_instances_terminated'
    waiting_for_master_resources_terminated = 'waiting_for_master_resources_terminated'
    cleanup_in_progress = 'cleanup_in_progress'
    cleanup_complete = 'cleanup_complete'
    no_instances_available = 'no_instances_available'
    
    
class SpotMasterFailureCode:
    """Master failure codes """
    max_attempts_exceeded = 'max_attempts_exceeded'
    
    
class TableSpotRequest:
    """Request table attributes """
    spot_request_uuid = 'spot_request_uuid'
    spot_master_uuid = 'spot_master_uuid'
    spot_request_id = 'spot_request_id'
    ts_last_state_check = 'ts_last_state_check'
    attempt_number = 'attempt_number'
    spot_price = 'spot_price'
    is_open = 'is_open'
    spot_request_state_code = 'spot_request_state_code'
    spot_request_failure_code = 'spot_request_failure_code'
    instance_id = 'instance_id'
    instance_public_ip_address = 'instance_public_ip_address'
    instance_username = 'instance_username'
    instance_termination_exception = 'instance_termination_exception'
    ts_start = 'ts_start'
    ts_end = 'ts_end'
    ts_heartbeat_daemon_started = 'ts_heartbeat_daemon_started'
    ts_heartbeat = 'ts_heartbeat'
    ts_rejected = 'ts_rejected'
    ts_granted = 'ts_granted'
    ts_terminated_request_recvd = 'ts_terminated_request_recvd'
    ts_terminated_instance = 'ts_terminated_instance'
    ts_cancel_recvd = 'ts_cancel_recvd'
    ts_cancel_complete = 'ts_cancel_complete'
    ts_pending_termination_detected = 'ts_pending_termination_detected'
    ts_pending_termination_detected = 'ts_pending_termination_detected'
    termination_time_exception = 'termination_time_exception'
    ts_cmd_complete = 'ts_cmd_complete'
    cmd_returncode = 'cmd_returncode'
    cmd_std_out = 'cmd_std_out'
    cmd_std_err = 'cmd_std_err'
    constraint_code = 'constraint_code'
    cmd_exception_message = 'cmd_exception_message'
    cmd_exception_traceback = 'cmd_exception_traceback'

    
class SpotRequestStateCode:
    """Request State Codes """
    spot_request_initiated = 'spot_request_initiated' 
    spot_request_in_progress = 'spot_request_in_progress' 
    instance_starting = 'instance_starting' 
    instance_running = 'instance_running' 
    instance_complete = 'instance_complete' 
    instance_complete_exception = 'instance_complete_exception' 
    instance_force_termination_pending = 'instance_force_termination_pending'
    instance_force_termination_exception = 'instance_force_termination_exception'
    instance_force_terminated = 'instance_force_terminated'
    instance_state_unknown = 'instance_state_unknown'
    constraint_encountered = 'constraint_encountered'
    
    
class SpotRequestFailureCode:
    """Request failure codes """
    request_id_not_found = 'request_id_not_found'               # the request id wasn't found - this should never happen...
    heartbeat_timeout_exceeded = 'heartbeat_timeout_exceeded'   # script on spot instance died, hung or looping
    terminated = 'terminated'                                   # price exceeded after spot started running, instance was terminated
    termination_time_exception = 'termination_time_exception'   # exception occured while getting metadata for termination time
    
    # Possible codes returned from get_all_spot_instance_requests taken from the Spot section in the EC2 manual
    capacity_not_available = 'capacity-not-available'
    capacity_oversubscribed = 'capacity-oversubscribed'
    price_too_low = 'price-too-low'
    not_scheduled_yet = 'not-scheduled-yet'
    launch_group_constraint = 'launch-group-constraint'
    az_group_constraint = 'az-group-constraint'
    placement_group_constraint = 'placement-group-constraint'
    constraint_not_fulfillable = 'constraint-not-fulfillable'

    
class TableSpotRSAKey:
    """RSA Key table attributes """
    spot_master_uuid = 'spot_master_uuid'
    rsa_key_encoded = 'rsa_key_encoded'

    
class TableSpotBatchJobParm:
    """SpotBatchJobParm table attributes """
    spot_master_uuid = 'spot_master_uuid'
    raw_batch_job_parm_item = 'raw_batch_job_parm_item'
    raw_user_job_parm_item = 'raw_user_job_parm_item'
