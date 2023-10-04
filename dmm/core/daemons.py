from multiprocessing import Process
from time import sleep, time
import logging

from dmm.utils.dbutil import get_request_by_status, mark_requests, subnet_allocation
from dmm.db.session import databased
from dmm.core.decision import NetworkGraph

from dmm.utils.config import config_get_int

import dmm.core.sense_api as sense_api

@databased
def stager_daemon(session=None):
    reqs_init = [req for req in get_request_by_status(status=["INIT"], session=session)]
    for req in reqs_init:
        # assign ips
        subnet_allocation(req, session=session)
        # create sense provision
        if req.src_ipv6_block != "best_effort":
            sense_link_id, _ = sense_api.stage_link(
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
    ng = NetworkGraph()
    for req in reqs_init:
        allocated_bandwidth = ng.get_bandwidth_for_request_id(req.request_id)
        print(allocated_bandwidth)
        req.update(
            {
                "bandwidth": int(allocated_bandwidth)
            }
        )
    # mark req as PREPARED
    mark_requests(reqs_init, "STAGED", session)

@databased
def provision_daemon(session=None):
    reqs_staged = [req for req in get_request_by_status(status=["STAGED"], session=session)]
    for req in reqs_staged:
        if req.src_ipv6_block != "best_effort":
            sense_api.provision_link(
                        req.sense_link_id, 
                        req.src_sense_uri,
                        req.dst_sense_uri,
                        req.src_ipv6_block,
                        req.dst_ipv6_block,
                        int(req.bandwidth),
                        alias=req.request_id
                    )
    mark_requests(reqs_staged, "PROVISIONED", session)

@databased
def reaper_daemon(session=None):
    for req in get_request_by_status(status=["FINISHED"], session=session):
        if (time() - req.updated_at) > 600:
            sense_api.delete_link(req.sense_link_id)
            req.delete(session)

def run_daemon(daemon, lock, frequency=60):
    while True:
        logging.info(f"Running {daemon.__name__}")
        with lock:
            try:
                daemon()
            except Exception as e:
                logging.error(f"{daemon.__name__} {e}")
        sleep(frequency)
        logging.info(f"{daemon.__name__} sleeping for {frequency} seconds")

def start_daemons(lock):

    daemon_frequency = config_get_int("dmm", "daemon_frequency", default=60)

    stager_process = Process(target=run_daemon, args=(stager_daemon, lock, daemon_frequency))
    provision_process = Process(target=run_daemon, args=(provision_daemon, lock, daemon_frequency))
    reaper_process = Process(target=run_daemon, args=(reaper_daemon, lock, daemon_frequency))

    stager_process.start()
    provision_process.start()
    reaper_process.start()