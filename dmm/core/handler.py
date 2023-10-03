import logging
import json

from dmm.utils.helpers import get_request_id

from dmm.utils.dbutil import get_request_from_id, mark_requests
from dmm.db.models import Request, FTSTransfer
from dmm.db.session import databased

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
                new_request.save(session)
    logging.info("Closing Preparer Handler")

# this will need to communicate with rucio immediately
# in rucio, check if transfer marked with sense activity, if yes, wait for dmm to return ips before submitting
@databased
def submitter_handler(payload, session=None):
    logging.info("Starting Submitter Handler")
    sense_map = {}
    for rule_id, submitter_reports in payload.items():
        sense_map[rule_id] = {}
        for rse_pair_id, report in submitter_reports.items():
            # maybe get ips and return them and update database
            src_rse_name, dst_rse_name = rse_pair_id.split("&")
            request_id = get_request_id(rule_id, src_rse_name, dst_rse_name)
            req = get_request_from_id(request_id, session)
            req.update(
                {
                    "n_transfers_submitted": req.n_transfers_submitted + report["n_transfers_submitted"]
                }
            )
            mark_requests([req], "QUEUED", session)
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
            if req.n_transfers_finished == req.n_transfers_total:
                mark_requests([req], "FINISHED", session)
    logging.info("Closing Finisher Handler")

def handle_client(lock, connection, address):
    try:
        logging.info(f"Connection accepted from {address}")
        data = connection.recv(4096).decode()
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
