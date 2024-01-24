from datetime import datetime
import requests
from dmm.utils.db import mark_requests, get_site, get_request_by_status

from dmm.db.models import Request, Site, Endpoint
from dmm.db.session import databased

@databased
def preparer(client=None, daemon_frequency=60, certs=None, session=None):
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
                url = str(src_site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
                data = requests.get(url, cert=certs, verify=False).json()
                for block, hostname in data[src_site.name]["metadata"]["xrootd"].items():
                    new_endpoint = Endpoint(site=src_site.name,
                                            ip_block=block,
                                            hostname=hostname,
                                            in_use=False
                                            )
                    new_endpoint.save(session)
            if get_site(rule["rse_expression"], session=session) is None:
                dst_site = Site(name=rule["rse_expression"])
                dst_site.save(session)
                url = str(dst_site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
                data = requests.get(url, cert=certs, verify=False).json()
                for block, hostname in data[dst_site.name]["metadata"]["xrootd"].items():
                    new_endpoint = Endpoint(site=dst_site.name,
                                            ip_block=block,
                                            hostname=hostname,
                                            in_use=False
                                            )
                    new_endpoint.save(session)
            new_request.save(session)

@databased
def rucio_modifier(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        curr_prio_in_rucio = client.get_replication_rule(req.rule_id)["priority"]
        if req.priority != curr_prio_in_rucio:
            mark_requests([req], "STALE", session)

# updates request status in db, daemon just deregisters request
@databased
def finisher(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        status = client.get_replication_rule(req.rule_id)['state']
        if status == "OK":
            mark_requests([req], "FINISHED", session)
