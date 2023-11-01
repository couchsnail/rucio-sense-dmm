import logging
import json
import requests

from dmm.utils.config import config_get

def modify_link_config(req, max_active, min_active):
    url = config_get("fts", "fts_host")
    cert = (config_get("fts", "cert"), config_get("fts", "key"))
    capath = "/etc/grid-security/certificates/"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    src_url_no_port = "davs://" + req.src_url.split(":")[0]
    dst_url_no_port = "davs://" + req.dst_url.split(":")[0]

    data = {
        "symbolicname": "-".join([src_url_no_port, dst_url_no_port]),
        "source": src_url_no_port,
        "destination": dst_url_no_port,
        "max_active": max_active,
        "min_active": min_active,
        "nostreams": 0,
        "optimizer_mode": 0,
        "no_delegation": False,
        "tcp_buffer_size": 0
    }
    
    data = json.dumps(data)
    response = requests.post(url + "/config/links/", headers=headers, cert=cert, verify=capath, data=data)
    logging.info(f"FTS link config modified, response: {response}")

def modify_se_config(req, max_inbound, max_outbound):
    url = config_get("fts", "fts_host")
    cert = (config_get("fts", "cert"), config_get("fts", "key"))
    capath = "/etc/grid-security/certificates/"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    src_url_no_port = "davs://" + req.src_url.split(":")[0]
    dst_url_no_port = "davs://" + req.dst_url.split(":")[0]

    data = {
        src_url_no_port: {
            "se_info": {
                "inbound_max_active": None,
                "inbound_max_throughput": None,
                "outbound_max_active": max_outbound,
                "outbound_max_throughput": None,
                "udt": None,
                "ipv6": None,
                "se_metadata": None,
                "site": None,
                "debug_level": None,
                "eviction": None
            }
        },
        dst_url_no_port: {
            "se_info": {
                "inbound_max_active": max_inbound,
                "inbound_max_throughput": None,
                "outbound_max_active": None,
                "outbound_max_throughput": None,
                "udt": None,
                "ipv6": None,
                "se_metadata": None,
                "site": None,
                "debug_level": None,
                "eviction": None
            }
        }
    }
    
    data = json.dumps(data)
    response = requests.post(url + "/config/se/", headers=headers, cert=cert, verify=capath, data=data)
    logging.info(f"FTS storage config modified, response: {response}")