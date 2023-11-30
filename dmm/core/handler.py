import logging
import json
import ipaddress

from dmm.utils.common import get_request_id
from dmm.utils.db import get_request_from_id, mark_requests, get_site, get_request_by_status, update_bandwidth, get_url_from_block
from dmm.db.models import Request, Site, FTSTransfer
from dmm.db.session import databased
from dmm.utils.sense import get_allocation

def subnet_allocation(req, session=None):
    reqs_finished = [req_fin for req_fin in get_request_by_status(status=["FINISHED"], session=session)]

    for req_fin in reqs_finished:
        if (req_fin.src_site == req.src_site and req_fin.dst_site == req.dst_site):
            req.update({
                "src_ipv6_block": req_fin.src_ipv6_block,
                "dst_ipv6_block": req_fin.dst_ipv6_block,
                "src_url": req_fin.src_url,
                "dst_url": req_fin.dst_url,
                "transfer_status": "ALLOCATED"
            })
            mark_requests([req_fin], "DELETED", session)
            return

    if req.priority != 0:
        src_ip_block = get_allocation(req.src_site, req.rule_id+"_"+req.src_site)
        dst_ip_block = get_allocation(req.dst_site, req.rule_id+"_"+req.dst_site)

    src_ip_block = str(ipaddress.IPv6Network(src_ip_block))
    dst_ip_block = str(ipaddress.IPv6Network(dst_ip_block))

    src_url = get_url_from_block(req.src_site, src_ip_block, session=session)
    dst_url = get_url_from_block(req.dst_site, dst_ip_block, session=session)

    req.update({
        "src_ipv6_block": src_ip_block,
        "dst_ipv6_block": dst_ip_block,
        "src_url": src_url,
        "dst_url": dst_url,
        "transfer_status": "ALLOCATED"
    })

@databased
def preparer_handler(payload, session=None):
    logging.info("Starting Preparer Handler")
    for rule_id, prepared_rule in payload.items():
        for rse_pair_id, request_attr in prepared_rule.items():
            src_rse_name, dst_rse_name = rse_pair_id.split("&")
            # Check if request has already been processed
            request_id = get_request_id(rule_id, src_rse_name, dst_rse_name)
            existing_req = get_request_from_id(request_id, session)
            if existing_req:
                existing_req.update(
                    {
                        "n_bytes_total": existing_req.n_bytes_total + request_attr["n_bytes_total"],
                        "n_transfers_total": existing_req.n_transfers_total + request_attr["n_transfers_total"]
                    }
                )
            else:
                new_request = Request(rule_id=rule_id, 
                                        src_site=src_rse_name, 
                                        dst_site=dst_rse_name,
                                        transfer_status="INIT", 
                                        **request_attr)
                if get_site(src_rse_name, session=session) is None:
                    src_site = Site(name=src_rse_name)
                    src_site.save(session)
                if get_site(dst_rse_name, session=session) is None:
                    dst_site = Site(name=dst_rse_name)
                    dst_site.save(session)
                subnet_allocation(new_request, session=session)
                # Commit to session
                new_request.save(session)
    logging.info("Closing Preparer Handler")

@databased
def submitter_handler(payload, session=None):
    logging.info("Starting Submitter Handler")
    sense_map = {}
    for rule_id, submitter_reports in payload.items():
        sense_map[rule_id] = {}
        for rse_pair_id, report in submitter_reports.items():
            src_rse_name, dst_rse_name = rse_pair_id.split("&")
            request_id = get_request_id(rule_id, src_rse_name, dst_rse_name)
            req = get_request_from_id(request_id, session)
            req.update(
                {
                    "n_transfers_submitted": req.n_transfers_submitted + report["n_transfers_submitted"]
                }
            )
            sense_map[rule_id][rse_pair_id] = {
                req.src_site: req.src_url,
                req.dst_site: req.dst_url
            }
    data = json.dumps(sense_map)
    logging.info("Closing Submitter Handler")
    return data

# updates request status in db, daemon just deregisters request
@databased
def finisher_handler(payload, session=None):
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

def handle_client(lock, connection, address):
    try:
        logging.info(f"Connection accepted from {address}")
        data = connection.recv(8192).decode()
        if not data:
            return
        data = json.loads(data)
        logging.debug(f"Received {data}")
        daemon = data["daemon"]
        if daemon.upper() == "PREPARER":
            with lock:
                preparer_handler(data["data"])
        elif daemon.upper() == "SUBMITTER":
            with lock:
                result = submitter_handler(data["data"])
                connection.send(result.encode())
        elif daemon.upper() == "FINISHER":
            with lock:
                finisher_handler(data["data"])
    except Exception as e:
        logging.error(f"Error processing client {address}: {str(e)}")
    finally:
        connection.close()
