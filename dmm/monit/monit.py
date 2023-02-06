import yaml
import requests
import logging
import time

class MonitConfig():
    """
    Get network metrics from Prometheus via its HTTP API and return aggregations of 
    those metrics
    """
    def __init__(self, configfile) -> None:
        with open(configfile, "r") as f_in:
            prometheus_config = yaml.safe_load(f_in).get("prometheus")
            prometheus_host = prometheus_config["host"]
            prometheus_port = prometheus_config["port"]
            # ftsmonit_config = yaml.safe_load(f_in).get("ftsmonit")
            # ftsmonit_host = ftsmonit_config["host"]
            # ftsmonit_port = ftsmonit_config["port"]
        self.prometheus_addr = f"http://{prometheus_host}:{prometheus_port}"
        # self.ftsmonit_addr = f"http://{ftsmonit_host}:{ftsmonit_port}"

def submit_prom_query(config, query_dict) -> dict:
    endpoint = "api/v1/query"
    query_addr = f"{config.prometheus_addr}/{endpoint}"
    return requests.get(query_addr, params=query_dict).json()

def get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["data"]["result"][0]["value"][1]
    
def get_interface(config, ipv6) -> str:
    response = submit_query(config.prometheus_addr, {"query": "node_network_address_info"})
    if response["status"] == "success":
        for metric in response["data"]["result"]:
            if metric["metric"]["address"] == ipv6:
                return [metric["metric"]["device"], metric["metric"]["instance"]]

def get_total_bytes_at_t(config, time, device, instance, rse_name) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    # TODO device_info
    params = f"device=\"{device}\",instance=\"{instance}\",job=~\".*{rse_name}.*\""
    metric = f"node_network_transmit_bytes_total{{{params}}}"
    # Get bytes transferred at the start time
    response = submit_query(config.prometheus_addr, {"query": metric, "time": time})
    if response["status"] == "success":
        bytes_at_t = get_val_from_response(response)
    else:
        raise Exception(f"query {metric} failed")
    return float(bytes_at_t)

def get_throughput_at_t(time, t_avg_over=None) -> float:
    if t_avg_over is None:
        t_avg_over = self.refresh_interval
    bytes_transmitted = sum([i * self.get_total_bytes_at_t(time + i * t_avg_over) for i in [-1,1]])
    # TODO account for bin edges 
    return bytes_transmitted/ (2 * t_avg_over)