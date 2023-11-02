import requests
from time import sleep

from dmm.utils.db import get_site
from dmm.utils.config import config_get

def get_request_id(rule_id, src_rse_name, dst_rse_name):
    return f"{rule_id}_{src_rse_name}_{dst_rse_name}"

def wait(condition, timeout):
    time = 0
    while ((not condition) and (time < timeout)):
        sleep(10)
        time += 10
    if time > timeout:
        return False
    
def get_site_ips(site, session=None):
    cert = config_get("dmm", "siterm_cert")
    key = config_get("dmm", "siterm_key")
    capath = "/etc/grid-security/certificates"
    
    site_ = get_site(site, session)
    data = requests.get(str(site_.query_url) + "/MAIN/sitefe/json/frontend/configuration", cert=(cert, key), verify=capath)

    return data["general"]["metadata"]["xrootd"]