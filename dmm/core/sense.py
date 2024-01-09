from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from ipaddress import IPv6Network

from dmm.utils.db import get_request_by_status, mark_requests, get_site
from dmm.utils.ip import get_url_from_block
from dmm.utils.fts import modify_link_config, modify_se_config
import dmm.utils.sense as sense

from dmm.db.session import databased

@databased
def allocation_daemon(session=None):
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
                    return

            src_ip_block = sense.get_allocation(new_request.src_site, "RUCIO_SENSE")
            dst_ip_block = sense.get_allocation(new_request.dst_site, "RUCIO_SENSE")

            src_ip_block = str(IPv6Network(src_ip_block))
            dst_ip_block = str(IPv6Network(dst_ip_block))

            src_url = get_url_from_block(new_request.src_site, src_ip_block, session)
            dst_url = get_url_from_block(new_request.dst_site, dst_ip_block, session)

            new_request.update({
                "src_ipv6_block": src_ip_block,
                "dst_ipv6_block": dst_ip_block,
                "src_url": src_url,
                "dst_url": dst_url,
                "transfer_status": "ALLOCATED"
            })

@databased
def stager_daemon(session=None):
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
def provision_daemon(session=None):
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
def sense_modifier_daemon(session=None):
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
def reaper_daemon(session=None):
    reqs_finished = [req for req in get_request_by_status(status=["FINISHED"], session=session)]
    for req in reqs_finished:
        if (datetime.utcnow() - req.updated_at).seconds > 600:
            sense.delete_link(req.sense_link_id)
            sense.free_allocation(req.src_site, req.rule_id)
            sense.free_allocation(req.dst_site, req.rule_id)
            req.delete(session)
            mark_requests([req], "DELETED", session)