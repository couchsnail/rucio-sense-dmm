import logging
import json
import requests

from dmm.utils.config import config_get

def modify_link_config(req, max_active, min_active):
    url = config_get("fts", "fts_host")
    cert = (config_get("fts", "cert"), config_get("fts", "key"))
    capath = "/etc/grid-security/certificates/"

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    data = {
        "symbolicname": "-".join([req.src_url, req.dst_url]),
        "source": req.src_url,
        "destination": req.dst_url,
        "max_active": max_active,
        "min_active": min_active,
        "nostreams": 0,
        "optimizer_mode": 0,
        "no_delegation": False,
        "tcp_buffer_size": 0
    }
    
    data = json.dumps(data)
    
    response = requests.post(url, headers=headers, cert=cert, verify=capath, data=data)
    
    return response.status_code

def delete_link_config(req):
    base_url = config_get("fts", "fts_host")
    cert = (config_get("fts", "cert"), config_get("fts", "key"))
    capath = "/etc/grid-security/certificates/"

    full_url = base_url + "/" + "-".join([req.src_url, req.dst_url])

    response = requests.delete(full_url, cert=cert, verify=capath)

    return response.status_code