import json
import yaml
import uuid

PROFILE_UUID = "ddd1dec0-83ab-4d08-bca6-9a83334cd6db"

def good_response(response):
    return len(response) == 0 or "ERROR" in response or "error" in response

def get_ipv6_pool(uri):
    """Return a list of IPv6 subnets at given site

    Note: not fully supported by SENSE yet
    """
    ipv6_pool = [
        "fc00::0010/124", "fc00::0020/124", "fc00::0030/124", 
        "fc00::0040/124", "fc00::0050/124"
    ]
    return ipv6_pool

def get_uplink_capacity(uri):
    """Return the maximum uplink capacity in Mb/s for a given site

    Notes: not fully supported by SENSE yet
    """
    return 100000

def get_uri(rse_name, full=True):
    """Return the root SENSE URI for a given Rucio RSE"""
    full_uri = f"urn:ogf:network:{rse_name.lower()}.foo:{rse_name}"
    if full:
        return full_uri
    else:
        return __get_rooturi(full_uri)

def __get_rooturi(full_uri):
    """Return the root SENSE URI for a given full SENSE URI"""
    return full_uri.split(":")[0]

def get_theoretical_bandwidth(src_uri, dst_uri, instance_uuid=PROFILE_UUID):
    """Return the maximum theoretical bandwidth available between two sites

    Note: not fully supported by SENSE yet
    """
    return uuid.uuid4(), 100000000000

def create_link(src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth, 
                instance_uuid=PROFILE_UUID, alias=""):
    """Create a SENSE guaranteed-bandwidth link between two sites"""
    return

def delete_link(instance_uuid):
    """Delete a SENSE link"""
    return

def reprovision_link(old_instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, 
                     new_bandwidth, alias=""):
    """Reprovision a SENSE link"""
    return uuid.uuid4()
