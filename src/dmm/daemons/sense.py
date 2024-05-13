import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dmm.utils.db import get_requests, mark_requests, get_site, free_endpoint, update_sense_circuit_status, get_vlan_range
import dmm.utils.sense as sense

from dmm.db.session import databased

@databased
def status_updater(session=None):
    reqs_provisioned = [req for req in get_requests(status=["STAGED", "PROVISIONED", "CANCELED", "STALE", "DECIDED", "FINISHED"], session=session)]
    for req in reqs_provisioned:
        status = sense.get_sense_circuit_status(req.sense_uuid)
        update_sense_circuit_status(req, status, session=session)

@databased
def stager(session=None):
    def stage_sense_link(req, session):
        try:
            # sense_uuid, max_bandwidth = sense.stage_link(
            #     src_uri=get_site(req.src_site, attr="sense_uri", session=session),
            #     dst_uri=get_site(req.dst_site, attr="sense_uri", session=session),
            #     src_ipv6=req.src_ipv6_block,
            #     dst_ipv6=req.dst_ipv6_block,
            #     vlan_range=get_vlan_range(req.src_site, req.dst_site, session=session),
            #     instance_uuid="",
            #     alias=req.rule_id
            # )
            sense_uuid, max_bandwidth = "test", 100000
            req.update({"sense_uuid": sense_uuid, "max_bandwidth": max_bandwidth})
            mark_requests([req], "STAGED", session)
        except Exception as e:
            logging.error(f"Failed to stage link for {req.rule_id}, {e}, will try again")
    reqs_init = [req for req in get_requests(status=["ALLOCATED"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_init:
            executor.submit(stage_sense_link, req, session)
    
@databased
def provision(session=None):
    def provision_sense_link(req, session):
        try:
            # sense.provision_link(
            #     instance_uuid=req.sense_uuid,
            #     src_uri=get_site(req.src_site, attr="sense_uri", session=session),
            #     dst_uri=get_site(req.dst_site, attr="sense_uri", session=session),
            #     src_ipv6=req.src_ipv6_block,
            #     dst_ipv6=req.dst_ipv6_block,
            #     bandwidth=int(req.bandwidth),
            #     vlan_range=get_vlan_range(req.src_site, req.dst_site, session=session),
            #     alias=req.rule_id
            # )
            # mark_requests([req], "PROVISIONED", session)
            pass
        except Exception as e:
            logging.error(f"Failed to provision link for {req.rule_id}, {e}, will try again")
    reqs_decided = [req for req in get_requests(status=["DECIDED"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_decided:
            executor.submit(provision_sense_link, req, session)

@databased
def sense_modifier(session=None):
    def modify_sense_link(req):
        try:
            # sense.modify_link(
            #     instance_uuid=req.sense_uuid,
            #     src_uri=get_site(req.src_site, attr="sense_uri", session=session),
            #     dst_uri=get_site(req.dst_site, attr="sense_uri", session=session),
            #     src_ipv6=req.src_ipv6_block,
            #     dst_ipv6=req.dst_ipv6_block,
            #     bandwidth=int(req.bandwidth),
            #     vlan_range=get_vlan_range(req.src_site, req.dst_site, session=session),
            #     alias=req.rule_id
            # )
            pass
        except Exception as e:
            logging.error(f"Failed to modify link for {req.rule_id}, {e}, will try again")
        # finally:
        #     mark_requests([req], "PROVISIONED", session)
    reqs_stale = [req for req in get_requests(status=["STALE"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_stale:
            executor.submit(modify_sense_link, req)

@databased
def canceller(session=None):
    reqs_finished = [req for req in get_requests(status=["FINISHED"], session=session)]
    for req in reqs_finished:
        if (datetime.utcnow() - req.updated_at).seconds > 60:
            try:
                sense.cancel_link(instance_uuid=req.sense_uuid)
                free_endpoint(req.src_url, session=session)
                free_endpoint(req.dst_url, session=session)
                mark_requests([req], "CANCELED", session=session)
            except Exception as e:
                logging.error(f"Failed to cancel link for {req.rule_id}, {e}, will try again")

@databased
def deleter(session=None):
    reqs_cancelled = [req for req in get_requests(status=["CANCELED"], session=session)]
    for req in reqs_cancelled:
        try:
            sense.delete_link(instance_uuid=req.sense_uuid)
            mark_requests([req], "DELETED", session=session)
        except Exception as e:
            logging.error(f"Failed to delete link for {req.rule_id}, {e}, will try again")