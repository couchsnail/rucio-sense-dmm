import logging
import json
from datetime import datetime
from ipaddress import IPv6Network

from dmm.utils.db import get_request_from_id, mark_requests, get_site, get_request_by_status, update_bandwidth
from dmm.utils.ip import get_url_from_block
from dmm.utils.sense import get_allocation

from dmm.db.models import Request, Site
from dmm.db.session import databased

@databased
def preparer_daemon(client=None, daemon_frequency=60, session=None):
    rules = client.list_replication_rules()
    for rule in rules:
        if (rule["meta"] is not None) and ("sense" in rule["meta"]) and ((datetime.utcnow() - rule['created_at']).seconds < daemon_frequency):
            new_request = Request(rule_id=rule["id"], 
                                    src_site=rule["source_replica_expression"], 
                                    dst_site=rule["rse_expression"],
                                    priority=rule["priority"],
                                    n_bytes_total=rule["bytes"],
                                    transfer_status="INIT",
                                    )
            if get_site(rule["source_replica_expression"], session=session) is None:
                src_site = Site(name=rule["source_replica_expression"])
                src_site.save(session)
            if get_site(rule["rse_expression"], session=session) is None:
                dst_site = Site(name=rule["rse_expression"])
                dst_site.save(session)

        reqs_finished = [req_fin for req_fin in get_request_by_status(status=["FINISHED"], session=session)]

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

        src_ip_block = get_allocation(new_request.src_site, "RUCIO_SENSE")
        dst_ip_block = get_allocation(new_request.dst_site, "RUCIO_SENSE")

        src_ip_block = str(IPv6Network(src_ip_block))
        dst_ip_block = str(IPv6Network(dst_ip_block))

        src_url = get_url_from_block(new_request.src_site, src_ip_block, session=session)
        dst_url = get_url_from_block(new_request.dst_site, dst_ip_block, session=session)

        new_request.update({
            "src_ipv6_block": src_ip_block,
            "dst_ipv6_block": dst_ip_block,
            "src_url": src_url,
            "dst_url": dst_url,
            "transfer_status": "ALLOCATED"
        })

        new_request.save(session)
    logging.info("Closing Preparer Handler")

def rucio_modifier_daemon():
    pass

@databased
def submitter_daemon(session=None):
    logging.info("Starting Submitter Handler")
    logging.info("Closing Submitter Handler")
    return data

# updates request status in db, daemon just deregisters request
@databased
def finisher_daemon(session=None):
    logging.info("Starting Finisher Handler")
    for rule_id, finisher_reports in payload.items():
        for rse_pair_id, report in finisher_reports.items():
            # Get request
            src_rse_name, dst_rse_name = rse_pair_id.split("&")
            request_id = get_request_id(rule_id, src_rse_name, dst_rse_name)
            req = get_request_from_id(request_id, session)
            # Update request
            req.update(
                {
                    "n_transfers_finished": req.n_transfers_finished + report["n_transfers_finished"],
                    "n_bytes_transferred": req.n_bytes_transferred + report["n_bytes_transferred"],
                    "external_ids": [FTSTransfer(value=ext_id) for ext_id in report["external_ids"]]
                }
            )
            if req.n_transfers_finished >= req.n_transfers_total:
                mark_requests([req], "FINISHED", session)
                update_bandwidth(req, 1, session=session)
    logging.info("Closing Finisher Handler")
