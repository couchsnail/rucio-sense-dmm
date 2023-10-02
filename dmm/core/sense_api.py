import json
import re
import logging
from time import sleep

from dmm.utils.config import config_get

from sense.client.workflow_combined_api import WorkflowCombinedApi
from sense.client.discover_api import DiscoverApi

PROFILE_UUID = ""

def get_profile_uuid():
    global PROFILE_UUID
    if PROFILE_UUID == "":
        PROFILE_UUID = config_get("sense", "profile_uuid")
    logging.info(f"Using SENSE Profile: {PROFILE_UUID}")
    return PROFILE_UUID

def good_response(response):
    return len(response) > 0 and "ERROR" not in response and "error" not in response

def get_ipv6_pool(uri):
    """Return a list of IPv6 subnets at given site"""
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_ipv6pool_get(uri)
    logging.debug(response)
    if len(response) == 0 or "ERROR" in response:
        raise ValueError(f"Discover query failed for {uri}")
    else:
        response = json.loads(response)
        return response["routing"][0]["ipv6_subnet_pool"].split(",")

def get_uplink_capacity(uri):
    """Return the maximum uplink capacity in Mb/s for a given site"""
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_peers_get(uri)
    logging.debug(response)
    if not good_response(response):
        raise ValueError(f"Discover query failed for {uri}")
    else:
        response = json.loads(response)
        #return float(response["peer_points"][0]["port_capacity"])
        return 10000

def good_response(response):
    return bool(response and not any("ERROR" in r for r in response))

def get_uri(rse_name, regex=".*?", full=False):
    discover_api = DiscoverApi()
    response = discover_api.discover_lookup_name_get(rse_name, search="NetworkAddress")
    if not good_response(response):
        raise ValueError(f"Discover query failed for {rse_name}")
    response = json.loads(response)
    if not response["results"]:
        raise ValueError(f"No results for '{rse_name}'")
    matched_results = [result for result in response["results"] if re.search(regex, result["name/tag/value"])]
    if len(matched_results) == 0:
        raise ValueError(f"No results matched '{regex}'")
    full_uri = matched_results[0]["resource"]
    if full:
        return full_uri
    root_uri = discover_api.discover_lookup_rooturi_get(full_uri)
    if not good_response(root_uri):
        raise ValueError(f"Discover query failed for {full_uri}")
    return root_uri

def stage_link(src_uri, dst_uri, src_ipv6, dst_ipv6, instance_uuid="", alias=""):
    workflow_api = WorkflowCombinedApi()
    workflow_api.instance_new() if instance_uuid == "" else setattr(workflow_api, "si_uuid", instance_uuid)
    intent = {
        "service_profile_uuid": get_profile_uuid(),
        "queries": [
            {
                "ask": "edit",
                "options": [
                    {"data.connections[0].terminals[0].uri": src_uri}, {"data.connections[0].terminals[0].ipv6_prefix_list": src_ipv6},
                    {"data.connections[0].terminals[1].uri": dst_uri}, {"data.connections[0].terminals[1].ipv6_prefix_list": dst_ipv6},
                    {"data.connections[0].terminals[0].vlan_tag": "3895-3899"}, {"data.connections[0].terminals[1].vlan_tag": "3895-3899"}
                ]
            },
            {"ask": "maximum-bandwidth", "options": [{"name": "Connection 1"}]}
        ]
    }
    if alias:
        intent["alias"] = alias
    response = workflow_api.instance_create(json.dumps(intent))
    if not good_response(response):
        raise ValueError(f"SENSE query failed for {instance_uuid}")
    response = json.loads(response)
    for query in response["queries"]:
        if query["asked"] == "maximum-bandwidth":
            result = query["results"][0]
            if "bandwidth" not in result:
                raise ValueError(f"SENSE query failed for {instance_uuid}")
            return response["service_uuid"], float(result["bandwidth"])

def provision_link(instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth, alias=""):
    workflow_api = WorkflowCombinedApi()
    workflow_api.si_uuid = instance_uuid
    intent = {
        "service_profile_uuid": get_profile_uuid(),
        "queries": [
            {
                "ask": "edit",
                "options": [
                    {"data.connections[0].bandwidth.capacity": str(bandwidth)},
                    {"data.connections[0].terminals[0].uri": src_uri}, {"data.connections[0].terminals[0].ipv6_prefix_list": src_ipv6},
                    {"data.connections[0].terminals[1].uri": dst_uri}, {"data.connections[0].terminals[1].ipv6_prefix_list": dst_ipv6},
                    {"data.connections[0].terminals[1].uri": dst_uri}, {"data.connections[0].terminals[1].ipv6_prefix_list": dst_ipv6},
                    {"data.connections[0].terminals[0].vlan_tag": "3895-3899"}, {"data.connections[0].terminals[1].vlan_tag": "3895-3899"}
                ]
            }
        ]
    }
    if alias:
        intent["alias"] = alias
    response = workflow_api.instance_create(json.dumps(intent))
    if not good_response(response):
        raise ValueError(f"SENSE query failed for {instance_uuid}")
    response = json.loads(response)
    workflow_api.instance_operate("provision", sync="true")

def delete_link(instance_uuid):
    workflow_api = WorkflowCombinedApi()
    status = workflow_api.instance_get_status(si_uuid=instance_uuid)
    if "error" in status:
        raise ValueError(status)
    if not any(status.startswith(s) for s in ["CREATE", "REINSTATE", "MODIFY"]):
        raise ValueError(f"Cannot cancel an instance in status '{status}'")
    workflow_api.instance_operate("cancel", si_uuid=instance_uuid, sync="true", force=str("READY" not in status).lower())
    status = workflow_api.instance_get_status(si_uuid=instance_uuid)
    total_time = 0
    while "CANCEL - READY" not in status and total_time < 30:
        sleep(5)
        total_time += 5
    try:
        workflow_api.instance_delete(si_uuid=instance_uuid)
    except:
        raise Exception(f"Cancel operation disrupted; instance not deleted")

def reprovision_link(old_instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, 
                     new_bandwidth, alias=""):
    """Reprovision a SENSE link
    Note: this currently deletes the existing link, then creates a copy of the old link
          with the new bandwidth provision; this is NOT how it will ultimately be done 
          in production, but an actual reprovisioning is not currently supported
    """
    # Delete old link
    delete_link(old_instance_uuid)
    # Create new link with new bandwidth
    new_instance_uuid, _ = stage_link(
        src_uri, 
        dst_uri, 
        src_ipv6, 
        dst_ipv6, 
        alias=alias
    )
    provision_link(
        new_instance_uuid, 
        src_uri, 
        dst_uri, 
        src_ipv6, 
        dst_ipv6, 
        new_bandwidth,
        alias=alias
    )
    return new_instance_uuid
