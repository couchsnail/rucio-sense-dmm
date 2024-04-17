import re

from dmm.db.session import databased
from dmm.utils.db import get_requests, mark_fts_modified
from dmm.utils.fts import modify_link_config, modify_se_config

num_active = {
    "T2_US_SDSC": {
        "T2_US_Caltech_Test" : 200
    },
    "T1_US_FNAL": {
        "T2_US_Caltech_Test" : 1600
    }
}

@databased
def fts_modifier(session=None):
    reqs_new = [req for req in get_requests(status=["ALLOCATED"], session=session)]
    for allocated_req in reqs_new:
        if not allocated_req.fts_modified:
            num_streams = 20
            link_modified = modify_link_config(allocated_req, max_active=num_streams, min_active=num_streams)
            se_modified = modify_se_config(allocated_req, max_inbound=num_streams, max_outbound=num_streams)
            if link_modified and se_modified:
                mark_fts_modified(allocated_req, session=session)
            
    reqs = [req for req in get_requests(status=["PROVISIONED"], session=session)]
    for provisioned_req in reqs:
        if not provisioned_req.fts_modified and re.match(r"(CREATE|MODIFY|REINSTATE) - READY$", provisioned_req.sense_circuit_status):
            num_streams = num_active[provisioned_req.src_site][provisioned_req.dst_site]
            link_modified = modify_link_config(provisioned_req, max_active=num_streams, min_active=num_streams)
            se_modified = modify_se_config(provisioned_req, max_inbound=num_streams, max_outbound=num_streams)
            if link_modified and se_modified:
                mark_fts_modified(provisioned_req, session=session)