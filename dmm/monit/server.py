from fastapi import FastAPI
from monit import *
from os import environ
from time import time

app = FastAPI()

m = MonitConfig(configfile=environ.get("MONIT_CONFIG"))

@app.get("/query")
def get_total_bytes(ipv6: str, rse_name: str):
    dev, int = get_interface(m, ipv6)
    return get_total_bytes_at_t(m, time(), dev, instance, rse_name)