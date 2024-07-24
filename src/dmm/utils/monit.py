import json
import requests
import socket

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

#t_avg_over should be time when transfer ended for given rule_id
#time stamp rule ending - time stamp provision kicking in
#The IP address here is src_ipv6 + dst_ipv6 (only src_ipv6 for now)
def prom_get_throughput_at_t(time, ipv6, t_avg_over=None) -> float:
    bytes_transmitted = sum([i * prom_get_total_bytes_at_t(time + i * 0.5 * t_avg_over, ipv6) for i in [-1,1]])
    # TODO account for bin edges
    return bytes_transmitted / (t_avg_over)

def fts_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["hits"]["hits"][0]["_source"]["data"]

def fts_submit_job_query(rule_id):
    fts_host = config_get("fts", "monit_host")
    fts_token = config_get("fts", "monit_auth_token")
    headers = {"Authorization": f"Bearer {fts_token}", "Content-Type": "application/json"}
    endpoint = "api/datasources/proxy/9233/monit_prod_fts_enr_complete*/_search"
    query_addr = f"{fts_host}/{endpoint}"
    data = {
        "size": 2,
        "query":{
            "bool":{
                "filter":[{
                    "query_string": {
                        "analyze_wildcard": "true",
                        "query": f"data.file_metadata.rule_id:{rule_id}"
                    }
                }]
            }
        },
        # "_source": ["data.tr_timestamp_start", "data.tr_timestamp_complete"]
    }
    data_string = json.dumps(data)
    response = requests.get(query_addr, data=data_string, headers=headers).json()
    timestamps = fts_get_val_from_response(response)
    return timestamps
    # fts_host = config_get("fts", "monit_host")
    # fts_token = config_get("fts", "monit_auth_token")
    # headers = {"Authorization": f"Bearer {fts_token}", "Content-Type": "application/json"}
    # endpoint = "api/datasources/proxy/9233/monit_prod_fts_enr_complete*/_search"
    # query_addr = f"{fts_host}/{endpoint}"
    # data = {
    #     "size": 100,
    #     "query":{
    #         "bool":{
    #             "filter":[{
    #                 "query_string": {
    #                     "analyze_wildcard": "true",
    #                     "query": f"data.file_metadata.rule_id:{rule_id}"
    #                 }
    #             }]
    #         }
    #     },
    #     "_source": ["data.tr_timestamp_start", "data.tr_timestamp_complete"]
    # }
    # data_string = json.dumps(data)
    # response = requests.get(query_addr, data=data_string, headers=headers).json()
    # timestamps = [hit["_source"]["data"] for hit in response["hits"]["hits"]]
    # return timestamps
    
'''
transfer state (might be redundant with circuit status), transfer start time,
                    transfer end time, transfer completion time, transfer operation time (how long it took),
                    transfer file size, volume of data transferred, number of retries, errors (if any)
'''
#Make sure to be able to call this from the rule_id on frontend (along with get_throughput_at_t)

#Objective: Find out if transfer failed/succeeded based on information in the query
                #Show the error
                #Correlate the metrics from FTS/Prometheus (do this after)

    #Throughput calculation: All FTS records that start after allocation timestamp
    #How much data is transferred at a given time - bytes/second
    #For a given rule, see the throughput (best effort, worst, etc.)
    #Need to dissect before and after - if transferring a petabyte and allocation takes 10 min
    #-See short time where throughput is bad, time when is large

    #find all transfers when throughput is low, separate from when all transfers are large
    #Middle timestamp are the ones you care about (how long it took to be transferred)
if __name__ == "__main__":
    result_query = fts_submit_job_query("95069e5365bd4381b9b2668ce739047b")
    print(result_query)
    # src_ip, dst_ip = result_query['src_hostname'], result_query['dst_hostname']
    # src_ipv6 = socket.getaddrinfo(src_ip,None)[-1][-1][0]
    # dst_ipv6 = socket.getaddrinfo(dst_ip,None)[-1][-1][0]
    # print(f"Src:{src_ipv6}, Dst: {dst_ipv6}")

    #For timestamp info: 
    #print(fts_submit_job_query("61b9e48e0de94ad394a6fe49d8560e5f"))

    