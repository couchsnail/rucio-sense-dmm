"""
SENSE Optimizer Prototype
"""
import json
import socket

from time import sleep

ADDRESS = ("localhost", 5000)

def sense_preparer(requests_with_sources):
    """
    Parse RequestWithSources objects collected by the preparer daemon and communicate 
    relevant info to SENSE via DMM

    :param requests_with_sources:    List of rucio.transfer.RequestWithSource objects
    """
    prepared_rules = {}
    for req_id, rws in requests_with_sources.items():
        # Check if rule has been accounted for
        if rws.rule_id not in prepared_rules.keys():
            prepared_rules[rws.rule_id] = {}
        # Check if RSE pair has been accounted for
        src_name = rws.sources[0] # FIXME: can we always take the first one?
        dst_name = rws.dest
        rse_pair_id = __get_rse_pair_id(src_name, dst_name)
        if rse_pair_id not in prepared_rules[rws.rule_id].keys():
            prepared_rules[rws.rule_id][rse_pair_id] = {
                "priority": rws.attributes["priority"],
                "n_transfers_total": 0,
                "n_bytes_total": 0
            }
        # Update request attributes
        prepared_rules[rws.rule_id][rse_pair_id]["n_transfers_total"] += 1
        prepared_rules[rws.rule_id][rse_pair_id]["n_bytes_total"] += rws.byte_count

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(ADDRESS)
        data = json.dumps({"daemon": "PREPARER", "data": prepared_rules})
        client.send(data.encode())

def sense_optimizer(t_files):
    """
    Replace source RSE hostname with SENSE link

    :param t_file:             t_file object from rucio.transfertool.fts3
    """
    # Count submissions and sort by rule ID and RSE pair ID
    submitter_reports = {}
    # Parse all file transfers
    for file_data in t_files:
        # Get rule ID
        rule_id = file_data["rule_id"]
        if rule_id not in submitter_reports.keys():
            submitter_reports[rule_id] = {}
        # Get RSE pair ID
        src_name = file_data["metadata"]["src_rse"]
        dst_name = file_data["metadata"]["dst_rse"]
        rse_pair_id = __get_rse_pair_id(src_name, dst_name)
        # Count transfers
        if rse_pair_id not in submitter_reports[rule_id].keys():
            submitter_reports[rule_id][rse_pair_id] = {
                "priority": file_data["priority"],
                "n_transfers_submitted": 0
            }
        submitter_reports[rule_id][rse_pair_id]["n_transfers_submitted"] += 1

    # Get SENSE mapping
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(ADDRESS)
        data = json.dumps({"daemon": "SUBMITTER", "data": submitter_reports})
        client.send(data.encode())
        sense_map = client.recv(4096).decode()
        print(sense_map)
        sense_map = json.loads(sense_map)

def sense_finisher(rule_id, replicas):
    """
    Parse replicas and update SENSE on how many jobs (per source+dest RSE pair) have 
    finished via DMM

    :param rule_id:     Rucio rule ID
    :param replicas:    Individual replicas produced by now-finished transfers
    """
    finisher_reports = {}
    for replica in replicas:
        src_name = replica.source
        dst_name = "T2_US_Caltech_Test"
        rse_pair_id = __get_rse_pair_id(src_name, dst_name) # FIXME: probably wrong
        if rse_pair_id not in finisher_reports.keys():
            finisher_reports[rse_pair_id] = {
                "n_transfers_finished": 0,
                "n_bytes_transferred": 0,
                "external_ids": []
            }
        finisher_reports[rse_pair_id]["n_transfers_finished"] += 1
        finisher_reports[rse_pair_id]["n_bytes_transferred"] += replica["bytes"]
        finisher_reports[rse_pair_id]["external_ids"].append(replica["external_id"])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(ADDRESS)
        data = json.dumps({"daemon": "FINISHER", "data": {rule_id: finisher_reports}})
        client.send(data.encode())

def __get_rse_pair_id(src_rse_name, dst_rse_name):
    return f"{src_rse_name}&{dst_rse_name}"

def __get_host_port(url):
    # Assumes the url is something like "protocol://hostname//path"
    # TODO: Need to make more universal for other url formats.
    return url.split("/")[2]


class RequestWithSources:
    def __init__(self, rule_id, sources, dest, attributes, byte_count):
        self.rule_id = rule_id
        self.dest = dest
        self.sources = sources
        self.attributes = attributes
        self.byte_count = byte_count

class TestSensePreparer:
    def test_sense_preparer(self):
        requests_with_sources = {
            1: RequestWithSources("RULEID1", ["T2_US_SDSC"], "T2_US_Caltech_Test", {"priority": 0}, 500),
            2: RequestWithSources("RULEID2", ["T2_US_SDSC"], "T2_US_Caltech_Test", {"priority": 2}, 500),
            3: RequestWithSources("RULEID3", ["T2_US_SDSC"], "T2_US_Caltech_Test", {"priority": 4}, 1000),
        }

        # Call the sense_preparer function with the test data
        sense_preparer(requests_with_sources)

class TFile:
    def __init__(self, rule_id, metadata, priority, sources, destinations, data):
        self.rule_id = rule_id
        self.metadata = metadata
        self.priority = priority
        self.sources = sources
        self.destinations = destinations
        self.data = data
    
    def __getitem__(self, key):
        return getattr(self, key, None)

class TestSenseOptimizer:
    def test_sense_optimizer(self):
        t_files = [
            TFile("RULEID2", {"src_rse": "T2_US_SDSC", "dst_rse": "T2_US_Caltech_Test"}, 3, ["source_url1"], ["destination_url1"], 'a'),
            TFile("RULEID3", {"src_rse": "T2_US_SDSC", "dst_rse": "T2_US_Caltech_Test"}, 3, ["source_url1"], ["destination_url1"], 'b'),
        ]
        # Call the sense_optimizer function with the test data
        sense_optimizer(t_files)

class Replica:
    def __init__(self, bytes, source, external_id):
        self.bytes = bytes
        self.source = source
        self.external_id = external_id

    def __getitem__(self, key):
        return getattr(self, key, None)

class TestSenseFinisher:
    def test_sense_finisher(self):
        rule_id = "rule1"
        replicas = [
            Replica(500, "T2_US_SDSC", "external_id1"),
            Replica(1000, "T2_US_SDSC", "external_id2"),
        ]

        # Call the sense_finisher function with the test data
        sense_finisher(rule_id, replicas)

if __name__ == "__main__":
    test_instance = TestSensePreparer()
    test_instance.test_sense_preparer()

    sleep(10)

    test_instance = TestSenseOptimizer()
    test_instance.test_sense_optimizer()

    # test_instance = TestSenseFinisher()
    # test_instance.test_sense_finisher()
