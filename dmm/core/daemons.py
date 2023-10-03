from multiprocessing import Process
from time import sleep, time
import logging

from dmm.utils.dbutil import get_request_by_status, mark_requests, subnet_allocation
from dmm.db.session import databased

import dmm.core.sense_api as sense_api

@databased
def stager_daemon(session=None):
    reqs_init = [req for req in get_request_by_status(status='INIT', session=session)]
    for req in reqs_init:
        # assign ips
        subnet_allocation(req, session=session)
        # create sense provision
        if req.src_ipv6_block != "best_effort":
            sense_link_id, theoretical_bandwidth = sense_api.stage_link(
                req.src_sense_uri,
                req.dst_sense_uri,
                req.src_ipv6_block,
                req.dst_ipv6_block,
                alias=req.request_id
            )
            req.update(
                {
                    "sense_link_id": sense_link_id,
                }
            )
    # mark req as PREPARED
    mark_requests(reqs_init, 'STAGED', session)

@databased
def modifier_daemon(session=None):
    pass

@databased
def finisher_daemon(session=None):
    pass

@databased
def reaper_daemon(session=None):
    for req in get_request_by_status(status="FINISHED", session=session):
        if (time() - req.updated_at) > 600:
            req.delete(session)

def run_daemon(daemon, lock, frequency=60):
    while True:
        logging.info(f"Running {daemon.__name__}")
        with lock:
            daemon()
        sleep(frequency)
        logging.info(f"{daemon.__name__} sleeping for {frequency} seconds")

def start_daemons(lock):
    stager_process = Process(target=run_daemon, args=(stager_daemon, lock, 5))
    modifier_process = Process(target=run_daemon, args=(modifier_daemon, lock, 60))
    finisher_process = Process(target=run_daemon, args=(finisher_daemon, lock, 60))
    reaper_process = Process(target=run_daemon, args=(reaper_daemon, lock, 60))

    stager_process.start()
    modifier_process.start()
    finisher_process.start()
    reaper_process.start()