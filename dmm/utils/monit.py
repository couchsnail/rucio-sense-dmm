import json
import requests

from dmm.utils.config import config_get

# Helper functions
def prom_submit_query(query_dict) -> dict:
    prometheus_user = config_get("prometheus", "user")
    prometheus_pass = config_get("prometheus", "password")
    prometheus_host = config_get("prometheus", "host")

    endpoint = "api/v1/query"
    query_addr = f"{prometheus_host}/{endpoint}"
    return requests.get(query_addr, params=query_dict, auth=(prometheus_user, prometheus_pass)).json()

def prom_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["data"]["result"][0]["value"][1]
    
def prom_get_interface(ipv6) -> str:
    response = prom_submit_query({"query": "node_network_address_info"})
    if response["status"] == "success":
        for metric in response["data"]["result"]:
            if metric["metric"]["address"] == ipv6:
                return (metric["metric"]["device"], metric["metric"]["instance"], metric["metric"]["job"], metric["metric"]["sitename"])

def prom_get_total_bytes_at_t(time, ipv6) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    device, instance, job, sitename = prom_get_interface(ipv6)
    query_params = f"device=\"{device}\",instance=\"{instance}\",job=\"{job}\",sitename=\"{sitename}\""
    print(query_params)
    metric = f"node_network_transmit_bytes_total{{{query_params}}}"
    # Get bytes transferred at the start time
    response = prom_submit_query({"query": metric, "time": time})
    print(response)
    if response is not None and response["status"] == "success":
        bytes_at_t = prom_get_val_from_response(response)
    else:
        raise Exception(f"query {metric} failed")
    return float(bytes_at_t)

def prom_get_throughput_at_t(time, ipv6, t_avg_over=None) -> float:
    bytes_transmitted = sum([i * prom_get_total_bytes_at_t(time + i * 0.5 * t_avg_over, ipv6) for i in [-1,1]])
    # TODO account for bin edges
    return bytes_transmitted / (t_avg_over)

def fts_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["hits"]["hits"][0]["_source"]["data"]

def fts_submit_job_query(rule_id):
    fts_host = config_get("fts-monit", "host")
    fts_token = config_get("fts-monit", "auth_token")
    headers = {"Authorization": f"Bearer {fts_token}", "Content-Type": "application/json"}
    endpoint = "api/datasources/proxy/9233/monit_prod_fts_enr_complete*/_search"
    query_addr = f"{fts_host}/{endpoint}"
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
        "_source": ["data.tr_timestamp_start", "data.tr_timestamp_complete"]
    }
    data_string = json.dumps(data)
    response = requests.get(query_addr, data=data_string, headers=headers).json()
    timestamps = fts_get_val_from_response(response)
    return timestamps

if __name__ == __main__:
    print(fts_submit_job_query("d8b4b3f6-ff2f-11eb-9e7c-0a580a81001b"))