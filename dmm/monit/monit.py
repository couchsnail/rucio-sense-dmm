import logging
import time
import json

from requests.sessions import Session

from dmm.utils.config import config_get

__MONITCONFIG = None

class MonitConfig():
    """
    Get network metrics from Prometheus via its HTTP API and return aggregations of 
    those metrics
    """
    def __init__(self) -> None:
        # Prometheus
        self.prometheus_host = config_get("prometheus", "host")
        prometheus_user = config_get("prometheus", "user")
        prometheus_pass = config_get("prometheus", "password")
        # create session with auth token
        self.prom_session = Session()
        self.prom_session.auth = (prometheus_user, prometheus_pass)

        # FTS
        self.fts_host = config_get("fts-monit", "host")
        fts_token = config_get("fts-monit", "auth_token")
        self.fts_session = Session()
        self.fts_session.headers.update({"Authorization": "Bearer {}".format(fts_token), "Content-Type": "application/json"})

# Helper functions
def prom_submit_query(config, query_dict) -> dict:
    endpoint = "api/v1/query"
    query_addr = f"{config.prometheus_addr}/{endpoint}"
    return config.prom_session.get(query_addr, params=query_dict).json()

def prom_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["data"]["result"][0]["value"][1]
    
def prom_get_interface(config, ipv6) -> str:
    response = prom_submit_query(config, {"query": "node_network_address_info"})
    if response["status"] == "success":
        for metric in response["data"]["result"]:
            if metric["metric"]["address"] == ipv6:
                return {metric["metric"]["device"], metric["metric"]["instance"]}

def prom_get_total_bytes_at_t(config, time, device, instance, rse_name) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    query_params = f"device=\"{device}\",instance=\"{instance}\",job=~\".*{rse_name}.*\""
    metric = f"node_network_transmit_bytes_total{{{query_params}}}"
    # Get bytes transferred at the start time
    response = prom_submit_query(config, {"query": metric, "time": time})
    print(response)
    if response is not None and response["status"] == "success":
        bytes_at_t = prom_get_val_from_response(response)
    else:
        raise Exception(f"query {metric} failed")
    return float(bytes_at_t)

def prom_get_throughput_at_t(config, time, device, instance, rse_name, t_avg_over=None) -> float:
    bytes_transmitted = sum([i * prom_get_total_bytes_at_t(config, time + i * 0.5 * t_avg_over, device, instance, rse_name) for i in [-1,1]])
    # TODO account for bin edges
    return bytes_transmitted / (t_avg_over)

def fts_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["hits"]["hits"][0]["_source"]["data"]

def fts_submit_job_query(config, job_id):
    endpoint = "api/datasources/proxy/9233/monit_prod_fts_enr_complete*/_search"
    query_addr = f"{config.fts_host}/{endpoint}"
    data = {
        "size":1,
        "query":{
            "bool":{
                "filter":[
                    {"query_string":{
                        "analyze_wildcard":"true",
                        "query":f"data.job_id:{job_id}"
                        }
                    }
                ]
            }
        },
        "_source": ['data.tr_timestamp_start', 'data.tr_timestamp_complete']
    }
    data_string = json.dumps(data)
    response = config.fts_session.get(query_addr, data=data_string).json()
    timestamps = fts_get_val_from_response(response)
    return timestamps

m = MonitConfig()
fts_submit_job_query(m, "1e420cc4-2b18-11ee-a358-fa163ece561c")