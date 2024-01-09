from datetime import datetime

from dmm.utils.db import mark_requests, get_site, get_request_by_status

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
            new_request.save(session)

@databased
def rucio_modifier_daemon(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        curr_prio_in_rucio = client.get_replication_rule(req.rule_id)["priority"]
        if req.priority != curr_prio_in_rucio:
            mark_requests([req], "STALE", session)

# updates request status in db, daemon just deregisters request
@databased
def finisher_daemon(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        status = client.get_replication_rule(req.rule_id)['state']
        if status == "OK":
            mark_requests([req], "FINISHED", session)
