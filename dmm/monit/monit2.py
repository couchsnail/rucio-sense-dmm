import yaml
import requests
import logging
import time

class Prometheus():
    """
    Get network metrics from Prometheus via its HTTP API and return aggregations of 
    those metrics
    """
    def __init__(self, ipv6, rse_name) -> None:
        with open("../config.yaml", "r") as f_in:
            prometheus_config = yaml.safe_load(f_in).get("prometheus")
            prometheus_host = prometheus_config["host"]
            prometheus_port = prometheus_config["port"]
        self.prometheus_addr = f"http://{prometheus_host}:{prometheus_port}"
        self.refresh_interval = 60 # in seconds
        self.t_req_create = time.time() 
        self._t_submitted = None
        self._t_provision = None
        # self.throughputs = {}
        self.ipv6 = ipv6
        self.rse_name = rse_name
        self.device_info = []
        # Update interface if prometheus address is reachable
        try:
            self.device_info = self.get_interface() 
        except requests.exceptions.ConnectionError as error:
            logging.warning(f"Prometheus unreachable - {error}")

    @property
    def t_submitted(self):
        return self._t_submitted
    
    @t_submitted.setter
    def t_submitted(self, t):
        self._t_submitted = t
    
    @property
    def t_provision(self):
        return self._t_provision
    
    @t_provision.setter
    def t_provision(self, t):
        self._t_provision = t

    @staticmethod
    def submit_query(prom_host, query_dict, endpoint="api/v1/query") -> dict:
        query_addr = f"{prom_host}/{endpoint}"
        return requests.get(query_addr, params=query_dict).json()
    
    @staticmethod 
    def get_val_from_response(response):
        """Extract desired value from typical location in Prometheus response"""
        return response["data"]["result"][0]["value"][1]
        
    def get_interface(self) -> str:
        response = self.submit_query(self.prometheus_addr, {"query": "node_network_address_info"})
        if response["status"] == "success":
            for metric in response["data"]["result"]:
                if metric["metric"]["address"] == self.ipv6:
                    return [metric["metric"]["device"], metric["metric"]["instance"]]

    def get_total_bytes_at_t(self, time) -> float:
        """
        Returns the total number of bytes transmitted from a given Rucio RSE via a given
        ipv6 address
        """
        params = f"device=\"{self.device_info[0]}\",instance=\"{self.device_info[1]}\",job=~\".*{self.rse_name}.*\""
        metric = f"node_network_transmit_bytes_total{{{params}}}"
        # Get bytes transferred at the start time
        response = self.submit_query(self.prometheus_addr, {"query": metric, "time": time})
        if response["status"] == "success":
            bytes_at_t = self.get_val_from_response(response)
        else:
            raise Exception(f"query {metric} failed")
        return float(bytes_at_t)

    def get_throughput_at_t(self, time, t_avg_over=None) -> float:
        if t_avg_over is None:
            t_avg_over = self.refresh_interval
        bytes_transmitted = sum([i * self.get_total_bytes_at_t(time + i * t_avg_over) for i in [-1,1]])
        # TODO account for bin edges 
        return bytes_transmitted/ (2 * t_avg_over)     

p = Prometheus("2001:48d0:3001:111::300", "T2_US_SDSC")
print(p.checkPrometheusConnectivity())
import time
print(p.get_interface())
print(p.get_throughput_at_t(time.time()-200))