from fastapi import FastAPI
from monit import get_throughput_at_t
from os import environ
from time import time

app = FastAPI()

m = MonitConfig(configfile=environ.get("MONIT_CONFIG"))

@app.post("/query")
def get_total_bytes(ipv6: str, rse_name: str):
    dev, int = get_interface(endpoint_1)
    return get_total_bytes_at_t(time(), dev, instance, rse_name)    