import requests
import logging

from dmm.utils.config import config_get
from dmm.utils.db import get_site

def get_url_from_block(site, ipv6_block, session=None):
    cert = config_get("dmm", "siterm_cert")
    key = config_get("dmm", "siterm_key")
    
    site_ = get_site(site, session)
    try:
        data = requests.get(str(site_.query_url) + "/MAIN/sitefe/json/frontend/configuration", cert=(cert, key), verify=False).json()
        return data[site]["metadata"]["xrootd"][ipv6_block]
    except:
        logging.error("Error while getting URL from block")
        return None