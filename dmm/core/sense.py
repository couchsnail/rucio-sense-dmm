from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dmm.utils.db import get_request_by_status, mark_requests, get_site
from dmm.utils.db import get_unused_endpoint
from dmm.utils.fts import modify_link_config, modify_se_config
import dmm.utils.sense as sense

from dmm.db.session import databased

@databased
def allocator(session=None):
    reqs_init = [req_init for req_init in get_request_by_status(status=["INIT"], session=session)]
    reqs_finished = [req_fin for req_fin in get_request_by_status(status=["FINISHED"], session=session)]
    for new_request in reqs_init:
        for req_fin in reqs_finished:
            if (req_fin.src_site == new_request.src_site and req_fin.dst_site == new_request.dst_site):
                new_request.update({
                    "src_ipv6_block": req_fin.src_ipv6_block,
                    "dst_ipv6_block": req_fin.dst_ipv6_block,
                    "src_url": req_fin.src_url,
                    "dst_url": req_fin.dst_url,
                    "transfer_status": "ALLOCATED"
                })
                mark_requests([req_fin], "DELETED", session)
                reqs_finished.remove(req_fin)
                break

        else:
            src_endpoint = get_unused_endpoint(new_request.src_site, session=session)
            dst_endpoint = get_unused_endpoint(new_request.dst_site, session=session)

            new_request.update({
                "src_ipv6_block": src_endpoint.ip_block,
                "dst_ipv6_block": dst_endpoint.ip_block,
                "src_url": src_endpoint.hostname,
                "dst_url": dst_endpoint.hostname,
                "transfer_status": "ALLOCATED"
            })

@databased
def stager(session=None):
    def stage_sense_link(req, session):
        sense_link_id, _ = sense.stage_link(
            get_site(req.src_site, attr="sense_uri", session=session),
            get_site(req.dst_site, attr="sense_uri", session=session),
            req.src_ipv6_block,
            req.dst_ipv6_block,
            instance_uuid="",
            alias=req.rule_id
        )
        req.update({"sense_link_id": sense_link_id})
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
            req.sense_link_id,
            get_site(req.src_site, attr="sense_uri", session=session),
            get_site(req.dst_site, attr="sense_uri", session=session),
            req.src_ipv6_block,
            req.dst_ipv6_block,
            int(req.bandwidth),
            alias=req.rule_id
        )
        modify_link_config(req, max_active=500, min_active=500)
        modify_se_config(req, max_inbound=500, max_outbound=500)
        mark_requests([req], "PROVISIONED", session)
    reqs_decided = [req for req in get_request_by_status(status=["DECIDED"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_decided:
            executor.submit(provision_sense_link, req, session)

@databased
def sense_modifier(session=None):
    def modify_sense_link(req):
        sense.modify_link(
            req.sense_link_id,
            int(req.bandwidth),
            alias=req.rule_id
        )
        pass
    reqs_stale = [req for req in get_request_by_status(status=["STALE"], session=session)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for req in reqs_stale:
            executor.submit(modify_sense_link, req)
            mark_requests([req], "PROVISIONED", session)

@databased
def reaper(session=None):
    reqs_finished = [req for req in get_request_by_status(status=["FINISHED"], session=session)]
    for req in reqs_finished:
        if (datetime.utcnow() - req.updated_at).seconds > 600:
            sense.delete_link(req.sense_link_id)
            sense.free_allocation(req.src_site, req.rule_id)
            sense.free_allocation(req.dst_site, req.rule_id)
            req.delete(session)
            mark_requests([req], "DELETED", session)