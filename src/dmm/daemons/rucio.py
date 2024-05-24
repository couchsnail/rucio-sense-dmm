from dmm.utils.db import mark_requests, get_requests, get_request_from_id, update_priority
from dmm.db.models import Request
from dmm.db.session import databased

import logging

@databased
def preparer(client=None, session=None):
    rules = client.list_replication_rules()
    logging.debug(f"Rucio got {len(rules)} rules: {rules}")
    for rule in rules:
        logging.debug(f"evaluating rule {rule['id']}")
        if (rule["meta"] is not None) and ("sense" in rule["meta"]) and (get_request_from_id(rule["id"], session=session) is None):
            new_request = Request(rule_id=rule["id"],
                                    src_site=rule["source_replica_expression"], 
                                    dst_site=rule["rse_expression"],
                                    priority=rule["priority"],
                                    transfer_status="INIT",
                                )
            new_request.save(session=session)
        else:
            logging.debug(f"rule {rule['id']} is not a sense rule")

@databased
def rucio_modifier(client=None, session=None):
    reqs = get_requests(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    if reqs == []:
        logging.debug("rucio_modifier: nothing to do")
        return
    req_prio_changed = False
    for req in reqs:
        curr_prio_in_rucio = client.get_replication_rule(req.rule_id)["priority"]
        if req.priority != curr_prio_in_rucio:
            req_prio_changed = True
            logging.debug(f"{req.rule_id} priority changed from {req.priority} to {curr_prio_in_rucio}")
            update_priority(req, curr_prio_in_rucio, session=session)
            mark_requests([req], "MODIFIED", session=session)
    if not req_prio_changed:
        logging.debug("No priority changes detected in rucio")

# updates request status in db, daemon just deregisters request
@databased
def finisher(client=None, session=None):
    reqs = get_requests(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED"], session=session)
    if reqs == []:
        logging.debug("finisher: nothing to do")
        return
    for req in reqs:
        status = client.get_replication_rule(req.rule_id)['state']
        if status in ["OK", "STUCK"]:
            logging.debug(f"Request {req.rule_id} finished with status {status}")
            mark_requests([req], "FINISHED", session=session)