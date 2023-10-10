from multiprocessing import Process
from time import sleep, time
import logging
import json

from dmm.utils.dbutil import get_request_by_status, mark_requests

from dmm.db.models import Request
from dmm.db.session import databased

import dmm.core.sense_api as sense_api

def subnet_allocation(req, session=None):
    with open("/opt/dmm/sites.json") as f:
        sites = json.load(f)

    src_site = sites.get(req.src_site, {})
    dst_site = sites.get(req.dst_site, {})

    src_ip_block = "best_effort"
    dst_ip_block = "best_effort"

    if req.priority != 0:
        src_allocated = {str(req.src_ipv6_block) for req in session.query(Request.src_ipv6_block).filter(Request.src_site == req.src_site).all()}
        dst_allocated = {str(req.dst_ipv6_block) for req in session.query(Request.dst_ipv6_block).filter(Request.dst_site == req.dst_site).all()}
        for ip_block, url in src_site.get("ipv6_pool", {}).items():
            if ip_block not in src_allocated:
                src_ip_block = ip_block
                break
        for ip_block, url in dst_site.get("ipv6_pool", {}).items():
            if ip_block not in dst_allocated:
                dst_ip_block = ip_block
                break
        src_url = src_site.get("ipv6_pool", {}).get(src_ip_block, "")
        dst_url = dst_site.get("ipv6_pool", {}).get(dst_ip_block, "")

    else:
        src_url = src_site.get("best_effort", "")
        dst_url = dst_site.get("best_effort", "")

    req.update({
        "src_ipv6_block": src_ip_block,
        "dst_ipv6_block": dst_ip_block,
        "src_url": src_url,
        "dst_url": dst_url
    })

@databased
def stager_daemon(session=None):
    reqs_init = [req for req in get_request_by_status(status=["INIT"], session=session)]
    for req in reqs_init:
        try:
            subnet_allocation(req, session=session)
            if req.src_ipv6_block != "best_effort":
                # sense_link_id = 'foo'
                sense_link_id, bandwidth = sense_api.stage_link(
                    req.src_sense_uri,
                    req.dst_sense_uri,
                    req.src_ipv6_block,
                    req.dst_ipv6_block,
                    instance_uuid="",
                    alias=req.request_id
                )
                req.update(
                    {
                        "sense_link_id": sense_link_id
                    }
                )
        except Exception as e:
            logging.error(f"Subnet Allocation Failed {e}")
    mark_requests(reqs_init, "STAGED", session)

@databased
def decision_daemon(network_graph=None, session=None):
    network_graph.update()
    reqs_staged = [req for req in get_request_by_status(status=["STAGED"], session=session)]
    for req in reqs_staged:
        if req.src_ipv6_block != "best_effort":
            allocated_bandwidth = network_graph.get_bandwidth_for_request_id(req.request_id)
            req.update(
                { 
                    "bandwidth": allocated_bandwidth
                }
            )
    mark_requests(reqs_staged, "DECIDED", session)

@databased
def provision_daemon(session=None):
    reqs_decided = [req for req in get_request_by_status(status=["DECIDED"], session=session)]
    for req in reqs_decided:
        if req.src_ipv6_block != "best_effort":
            pass
            # sense_api.provision_link(
            #             req.sense_link_id, 
            #             req.src_sense_uri,
            #             req.dst_sense_uri,
            #             req.src_ipv6_block,
            #             req.dst_ipv6_block,
            #             int(req.bandwidth),
            #             alias=req.request_id
            #         )
    mark_requests(reqs_decided, "PROVISIONED", session)

@databased
def reaper_daemon(session=None):
    for req in get_request_by_status(status=["FINISHED"], session=session):
        if (time() - req.updated_at) > 600:
            sense_api.delete_link(req.sense_link_id)
            req.delete(session)

def run_daemon(daemon, lock, frequency, **kwargs):
    while True:
        logging.info(f"Running {daemon.__name__}")
        with lock:
            try:
                daemon(**kwargs)
            except Exception as e:
                logging.error(f"{daemon.__name__} {e}")
        sleep(frequency)
        logging.info(f"{daemon.__name__} sleeping for {frequency} seconds")