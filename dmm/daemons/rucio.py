from dmm.utils.db import mark_requests, get_request_by_status, get_request_from_id, update_priority
from dmm.db.models import Request
from dmm.db.session import databased

@databased
def preparer(client=None, session=None):
    rules = client.list_replication_rules()
    for rule in rules:
        if (rule["meta"] is not None) and ("sense" in rule["meta"]) and (get_request_from_id(rule["id"], session=session) is None):
            new_request = Request(rule_id=rule["id"], 
                                    src_site=rule["source_replica_expression"], 
                                    dst_site=rule["rse_expression"],
                                    priority=rule["priority"],
                                    n_bytes_total=rule["bytes"],
                                    transfer_status="INIT",
                                    )
            new_request.save(session=session)

@databased
def rucio_modifier(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        curr_prio_in_rucio = client.get_replication_rule(req.rule_id)["priority"]
        if req.priority != curr_prio_in_rucio:
            update_priority(req, curr_prio_in_rucio, session=session)
            mark_requests([req], "MODIFIED", session=session)

# updates request status in db, daemon just deregisters request
@databased
def finisher(client=None, session=None):
    reqs = get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    for req in reqs:
        status = client.get_replication_rule(req.rule_id)['state']
        if status == "OK":
            mark_requests([req], "FINISHED", session=session)