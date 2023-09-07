from dmm.utils.config import config_get, config_get_bool, config_get_int

from dmm.core.site import Site
from dmm.core.request import Request
from dmm.core.orchestrator import Orchestrator

import threading
import logging
import socket
import json

class DMM:
    def __init__(self, n_workers=4):
        # Config attrs
        self.host = config_get("dmm", "host", default='localhost')
        self.port = config_get_int("dmm", "port", default=5000)
        self.monitoring = config_get_bool("dmm", "monitoring", default=False)

        self.orchestrator = Orchestrator(n_workers=n_workers)
        self.lock = threading.Lock()
        self.sites = {}
        self.requests = {}

    def __dump(self):
        for request in self.requests.values():
            logging.debug(
                f"{request} | "
                f"{request.src_site.rse_name} --> {request.dst_site.rse_name} "
                f"{request.bandwidth} Mb/s"
            )

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
    def link_closer(request, monitoring):
        logging.debug(f"{request} | closing link")
        request.close_link()

    def update_requests(self, msg):
        """Update bandwidth provisions for all links"""
        logging.info("updating link bandwidth provisions and metadata")
        for request_id, request in self.requests.items():
            # Submit SENSE query
            link_updater_args = (
                request,
                msg if request.link_is_open else "opened link",
                self.monitoring
            )
            self.orchestrator.put(request_id, DMM.link_updater, link_updater_args)

    def preparer_handler(self, payload):
        """
        Organize data (the 'payload') from Rucio preparer daemon into Request objects,
        where each Request == (Rucio Rule ID + RSE Pair), open new links, and 
        reprovision existing links appropriately
        
        payload = {
            rule_id_1: {
                "SiteA&SiteB": {
                    "transfer_ids": [str, str, ...],
                    "priority": int,
                    "n_bytes_total": int,
                    "n_transfers_total": int
                },
                "SiteB&SiteC": { ... },
                ...
            },
            rule_id_2: { ... },
            ...
        }
        """
        logging.info("Starting Preparer Handler")
        for rule_id, prepared_rule in payload.items():
            for rse_pair_id, request_attr in prepared_rule.items():
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                # Check if request has already been processed
                request_id = Request.id(rule_id, src_rse_name, dst_rse_name)
                if request_id in self.requests.keys():
                    logging.error("request ID already processed--should never happen!")
                    continue
                # Retrieve or construct source Site object
                src_site = self.sites.get(src_rse_name, Site(src_rse_name))
                if src_rse_name not in self.sites.keys():
                    self.sites[src_rse_name] = src_site
                # Retrieve or construct destination Site object
                dst_site = self.sites.get(dst_rse_name, Site(dst_rse_name))
                if dst_rse_name not in self.sites.keys():
                    self.sites[dst_rse_name] = dst_site
                # Create new Request
                request = Request(rule_id, src_site, dst_site, **request_attr)
                request.register()
                # Store new request and its corresponding link
                self.requests[request_id] = request

        self.update_requests("accommodating for new requests")
        logging.info("Closing Preparer Handler")

    def submitter_handler(self, payload):
        """
        Return the IPv6 pair (source and dest) for a the request being submitted by the 
        Rucio submitter daemon
        
        payload = {
            rule_id_1: {
                "SiteA&SiteB": {
                    "priority": int,
                    "n_transfers_submitted": int
                },
                "SiteB&SiteC": { ... },
                ...
            },
            rule_id_2: { ... },
            ...
        }
        """
        logging.info("Starting Submitter Handler")
        n_priority_changes = 0
        sense_map = {}
        for rule_id, submitter_reports in payload.items():
            sense_map[rule_id] = {}
            for rse_pair_id, report in submitter_reports.items():
                # Get request
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                request_id = Request.id(rule_id, src_rse_name, dst_rse_name)
                req = self.requests[request_id]
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

    def finisher_handler(self, payload):
        """
        Parse data (the 'payload') from Rucio finisher daemon, update progress of 
        every request, close the links for any that have finished, and reprovision 
        existing links if possible
        
        payload = {
            rule_id_1: {
                "SiteA&SiteB": {
                    "n_transfers_finished": int,
                    "n_bytes_transferred": int
                },
                "SiteB&SiteC": { ... },
                ...
            },
            rule_id_2: { ... },
            ...
        }
        """
        logging.info("Starting Finisher Handler")
        n_link_closures = 0
        for rule_id, finisher_reports in payload.items():
            for rse_pair_id, report in finisher_reports.items():
                # Get request
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                request_id = Request.id(rule_id, src_rse_name, dst_rse_name)
                request = self.requests[request_id]
                # # Check performance and request logs
                report["external_ids"]
                # Update request
                request.n_transfers_finished += report["n_transfers_finished"]
                request.n_bytes_transferred += report["n_bytes_transferred"]
                if request.n_transfers_finished == request.n_transfers_total:
                    request.deregister()
                    # Stage the link for closure
                    closer_args = (request, self.monitoring)
                    self.orchestrator.clear(request_id)
                    self.orchestrator.put(request_id, DMM.link_closer, closer_args)
                    n_link_closures += 1
                    # Clean up
                    self.requests.pop(request_id)

        if n_link_closures > 0:
            self.update_requests("adjusting for request deletion")
        logging.info("Closing Finisher Handler")