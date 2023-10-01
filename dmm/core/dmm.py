from dmm.utils.config import config_get, config_get_bool, config_get_int
from dmm.utils.helpers import *

from dmm.core.site import Site
from dmm.core.request import Request
from dmm.core.orchestrator import Orchestrator

from dmm.sql.models import Request, FTSTransfer
from dmm.sql.session import databased

import threading
import logging
import socket
import json

class DMM:
    def __init__(self, n_workers=4):
        # Config attrs
        self.host = config_get("dmm", "host", default='localhost')
        self.port = config_get_int("dmm", "port", default=5000)

        self.orchestrator = Orchestrator(n_workers=n_workers)
        self.lock = threading.Lock()
        self.sites = {}

    def stop(self):
        self.orchestrator.stop()
        return

    def handle_client(self, connection, address):
        try:
            logging.info(f"Connection accepted from {address}")
            data = connection.recv(4096).decode()
            if not data:
                return
            data = json.loads(data)
            logging.info(f"Received {data}")
            daemon = data["daemon"]
            if daemon.upper() == "PREPARER":
                with self.lock:
                    self.preparer_handler(data["data"])
            elif daemon.upper() == "SUBMITTER":
                with self.lock:
                    result = self.submitter_handler(data["data"])
                    logging.debug(f"Sending {result} to Rucio Submitter")
                    connection.send(result.encode())
            elif daemon.upper() == "FINISHER":
                with self.lock:
                    self.finisher_handler(data["data"])
        except Exception as e:
            logging.error(f"Error processing client {address}: {str(e)}")
        finally:
            connection.close()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((self.host, self.port))
            listener.listen(1)
            logging.info(f"Listening on {self.host}:{self.port}")
            while True:
                logging.info("Waiting for the next connection")
                connection, address = listener.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(connection, address))
                client_thread.start()

    @staticmethod
    def link_updater(request, msg, monitoring):
        # Update link
        old_bandwidth = request.bandwidth
        if request.link_is_open:
            request.reprovision_link()
            changed = (old_bandwidth != request.bandwidth)
        else:
            request.open_link()
            changed = True
        # Update metadata
        if changed and not request.best_effort:
            logging.debug(f"{request} | {old_bandwidth} --> {request.bandwidth}; {msg}")

    @staticmethod
    def link_closer(request):
        logging.debug(f"{request} | closing link")
        request.close_link()

    def update_requests(self, msg):
        """Update bandwidth provisions for all links"""
        logging.info("updating link bandwidth provisions and metadata")
        for request in :
            # Submit SENSE query
            link_updater_args = (
                request,
                msg if request.link_is_open else "opened link"
                )
            self.orchestrator.put(request_id, DMM.link_updater, link_updater_args)

    @databased
    def get_request_from_id(request_id, session=None):
        return session.query(Request).filter(Request.request_id == request_id).first()

    @databased
    def get_request_by_status(status, session=None):
        return session.query(Request).filter(Request.transfer_status == status).all()

    # TODO change this so it only adds request to the db, actual prep will be done by daemon
    @databased
    def preparer_handler(self, payload, session=None):
        logging.info("Starting Preparer Handler")
        for rule_id, prepared_rule in payload.items():
            for rse_pair_id, request_attr in prepared_rule.items():
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                # Check if request has already been processed
                request_id = id(rule_id, src_rse_name, dst_rse_name)
                existing_req = self.get_request_from_id(request_id)
                if existing_req:
                    existing_req.update(
                        {
                            "n_bytes_total": existing_req.n_bytes_total + request_attr.n_bytes_total,
                            "n_transfers_total": existing_req.n_transfers_total + request_attr.n_transfers_total
                        }
                    )
                else:
                    new_request = Request(request_id, **request_attr)
                    new_request.save(session)
        logging.info("Closing Preparer Handler")

    # this will need to communicate with rucio immediately
    # in rucio, check if transfer marked with sense activity, if yes, wait for dmm to return ips before submitting
    def submitter_handler(self, payload):
        logging.info("Starting Submitter Handler")
        n_priority_changes = 0
        sense_map = {}
        for rule_id, submitter_reports in payload.items():
            sense_map[rule_id] = {}
            for rse_pair_id, report in submitter_reports.items():
                # Get request
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                request_id = id(rule_id, src_rse_name, dst_rse_name)
                req = self.get_request_from_id(request_id)
                # Update request
                req.n_transfers_submitted += report["n_transfers_submitted"]
                if report["priority"] != req.priority:
                    req.priority = report["priority"]
                    n_priority_changes += 1
                # Get SENSE link endpoints
                sense_map[rule_id][rse_pair_id] = {
                    # block_to_ipv6 translation is a hack; should not be needed in the future
                    req.src_site.rse_name: req.src_site.block_to_ipv6[req.src_ipv6],
                    req.dst_site.rse_name: req.dst_site.block_to_ipv6[req.dst_ipv6]
                }

        if n_priority_changes > 0:
            self.update_requests("adjusting for priority update")

        data = json.dumps(sense_map)
        logging.info("Closing Submitter Handler")
        return data

    # updates request status in db, daemon just deregisters request 
    def finisher_handler(self, payload):
        logging.info("Starting Finisher Handler")
        n_link_closures = 0
        for rule_id, finisher_reports in payload.items():
            for rse_pair_id, report in finisher_reports.items():
                # Get request
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                request_id = Request.id(rule_id, src_rse_name, dst_rse_name)
                request = self.requests[request_id]
                # Update request
                request.n_transfers_finished += report["n_transfers_finished"]
                request.n_bytes_transferred += report["n_bytes_transferred"]
                if request.n_transfers_finished == request.n_transfers_total:
                    request.deregister()
                    # Stage the link for closure
                    closer_args = (request)
                    self.orchestrator.clear(request_id)
                    self.orchestrator.put(request_id, DMM.link_closer, closer_args)
                    n_link_closures += 1
                    # Clean up
                    self.requests.pop(request_id)

        if n_link_closures > 0:
            self.update_requests("adjusting for request deletion")
        logging.info("Closing Finisher Handler")