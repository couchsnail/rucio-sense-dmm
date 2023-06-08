import yaml

from requests.sessions import Session

import logging
import time

__MONITCONFIG = None

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
            self.prometheus_user = prometheus_config["user"]
            self.prometheus_pass = prometheus_config["password"]
            # ftsmonit_config = yaml.safe_load(f_in).get("ftsmonit")
            # ftsmonit_host = ftsmonit_config["host"]
            # ftsmonit_port = ftsmonit_config["port"]
        self.prometheus_addr = f"http://{prometheus_host}:{prometheus_port}"
        # self.ftsmonit_addr = f"http://{ftsmonit_host}:{ftsmonit_port}"
        self.session = Session()
        self.session.auth = (self.prometheus_user, self.prometheus_pass)

# Helper functions
def prom_submit_query(config, query_dict) -> dict:
    endpoint = "api/v1/query"
    query_addr = f"{config.prometheus_addr}/{endpoint}"
    return config.session.get(query_addr, params=query_dict).json()

def prom_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["data"]["result"][0]["value"][1]
    
def prom_get_interface(config, ipv6) -> str:
    response = prom_submit_query(config, {"query": "node_network_address_info"})
    if response["status"] == "success":
        for metric in response["data"]["result"]:
            if metric["metric"]["address"] == ipv6:
                return [metric["metric"]["device"], metric["metric"]["instance"]]

def prom_get_total_bytes_at_t(config, time, device, instance, rse_name) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    params = f"device=\"{device}\",instance=\"{instance}\",job=~\".*{rse_name}.*\""
    metric = f"node_network_transmit_bytes_total{{{params}}}"
    # Get bytes transferred at the start time
    response = prom_submit_query(config, {"query": metric, "time": time})
    if response is not None and response["status"] == "success":
        bytes_at_t = prom_get_val_from_response(response)
    else:
        raise Exception(f"query {metric} failed")
    return float(bytes_at_t)

def prom_get_throughput_at_t(config, time, device, instance, rse_name, t_avg_over=None) -> float:
    bytes_transmitted = sum([i * prom_get_total_bytes_at_t(config, time + i * t_avg_over, device, instance, rse_name) for i in [-1,1]])
    # TODO account for bin edges 

    return bytes_transmitted / (2 * t_avg_over)

# def get_log_addr(self, transfer_id):
#     job = requests.get(f"{self.fts_host}/jobs/{transfer_id}/files",
#                         cert=self.cert, verify=self.verify, headers=self.headers)
#     if job and (job.status_code == 200 or job.status_code == 207):
#         file = job.json()[0]
#         return f"https://{file['transfer_host']}:8449{file['log_file']}"

# def write_log(transfer_id, log):
#     with open(f"/tmp/fts-transfer-{transfer_id}.log", 'w+') as file:
#         file.write(log)

# def log_request(self, transfer_ids):
#     for transfer_id in transfer_ids:
#         try:
#             log_addr = self.get_log_addr(transfer_id)
#             log = requests.get(log_addr, verify=self.verify)
#             if log and log.status_code == 200:
#                 self.write_log(transfer_id, log.text)
#         except:
#             logging.debug(f"Exception: Job {transfer_id} not found")



# if __name__ == "__main__":
#     configfile = "./config.yaml"
#     m = MonitConfig(configfile)
#     a = prom_get_total_bytes_at_t(m, time.time(), 'vlan.4071', 'k8s-gen4-02.sdsc.optiputer.net:9100', 'T2_US_SDSC')
#     b = prom_get_throughput_at_t(m, time.time()-30000, 'vlan.4071', 'k8s-gen4-02.sdsc.optiputer.net:9100', 'T2_US_SDSC', t_avg_over=1000)
#     print(b)