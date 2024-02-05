from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dmm.utils.db import get_request_by_status, mark_requests, get_site, free_endpoint
from dmm.utils.fts import modify_link_config, modify_se_config
import dmm.utils.sense as sense

from dmm.db.session import databased

@databased
def stager(session=None):
    def stage_sense_link(req, session):
        sense_uuid, max_bandwidth = sense.stage_link(
            get_site(req.src_site, attr="sense_uri", session=session),
            get_site(req.dst_site, attr="sense_uri", session=session),
            req.src_ipv6_block,
            req.dst_ipv6_block,
            instance_uuid="",
            alias=req.rule_id
        )
        req.update({"sense_uuid": sense_uuid, "max_bandwidth": max_bandwidth})
        modify_link_config(req, max_active=50, min_active=50)
        modify_se_config(req, max_inbound=50, max_outbound=50)
        mark_requests([req], "STAGED", session)
    reqs_init = [req for req in get_request_by_status(status=["ALLOCATED"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_init:
            executor.submit(stage_sense_link, req, session)
    
@databased
def provision(session=None):
    def provision_sense_link(req, session):
        sense.provision_link(
            req.sense_uuid,
            get_site(req.src_site, attr="sense_uri", session=session),
            get_site(req.dst_site, attr="sense_uri", session=session),
            req.src_ipv6_block,
            req.dst_ipv6_block,
            int(req.bandwidth),
            alias=req.rule_id
        )
        modify_link_config(req, max_active=1500, min_active=1500)
        modify_se_config(req, max_inbound=1500, max_outbound=1500)
        mark_requests([req], "PROVISIONED", session)
    reqs_decided = [req for req in get_request_by_status(status=["DECIDED"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_decided:
            executor.submit(provision_sense_link, req, session)

@databased
def sense_modifier(session=None):
    def modify_sense_link(req):
        sense.modify_link(
            req.sense_uuid,
            get_site(req.src_site, attr="sense_uri", session=session),
            get_site(req.dst_site, attr="sense_uri", session=session),
            req.src_ipv6_block,
            req.dst_ipv6_block,
            int(req.bandwidth),
            alias=req.rule_id
        )
    reqs_stale = [req for req in get_request_by_status(status=["STALE"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_stale:
            executor.submit(modify_sense_link, req)

@databased
def canceller(session=None):
    reqs_finished = [req for req in get_request_by_status(status=["FINISHED"], session=session)]
    for req in reqs_finished:
        if (datetime.utcnow() - req.updated_at).seconds > 60:
            sense.cancel_link(req.sense_uuid)
            free_endpoint(req.src_url, session=session)
            free_endpoint(req.dst_url, session=session)
            mark_requests([req], "CANCELLED", session=session)

@databased
def deleter(session=None):
    reqs_cancelled = [req for req in get_request_by_status(status=["CANCELLED"], session=session)]
    for req in reqs_cancelled:
        sense.cancel_link(req.sense_uuid)
        mark_requests([req], "DELETED", session=session)