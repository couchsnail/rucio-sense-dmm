import logging
from sqlalchemy import text, or_

from dmm.db.models import Request, Site, Endpoint, Mesh

# Requests
def get_request_from_id(rule_id, session=None):
    req = session.query(Request).filter(Request.rule_id == rule_id).first()
    return req if req else None

def get_requests(status=None, session=None):
    if status is not None:
        return session.query(Request).filter(Request.transfer_status.in_(status)).all()
    else:
        return session.query(Request).all()

def get_request_cursor(session=None):
    return session.execute(text("SELECT * from requests")).cursor

def mark_requests(reqs, status, session=None):
    for req in reqs:
        req.update({
            "transfer_status": status,
            "fts_modified": False
        })
        logging.debug(f"Marked {req.rule_id} as {status}")

def update_bandwidth(req, bandwidth, session=None):
    req.update({
        "bandwidth": bandwidth
    })
    logging.debug(f"Updated bandwidth to {bandwidth} for {req.rule_id}")

def update_priority(req, priority, session=None):
    req.update({
        "priority": priority,
        "modified_priority": priority
    })
    logging.debug(f"Updated priority to {priority} for {req.rule_id}")

def update_sense_circuit_status(req, status, session=None):
    req.update({
        "sense_circuit_status": status
    })
    logging.debug(f"Updated sense_circuit_status to {status} for {req.rule_id}")

def mark_fts_modified(req, session=None):
    req.update({
        "fts_modified": True
    })
    logging.debug(f"Marked fts_modified for {req.rule_id}")

# Sites
def get_site(site_name, attr=None, session=None):
    if attr:
        query = session.query(Site).filter(Site.name == site_name).first()
        return getattr(query, attr)
    else:
        return session.query(Site).filter(Site.name == site_name).first()    

# Endpoints
def get_all_endpoints(session=None):
    endpoints = session.query(Endpoint).all()
    return endpoints

def get_endpoint(hostname, session=None):
    endpoint = session.query(Endpoint).filter(Endpoint.hostname == hostname).first()
    return endpoint

def get_unused_endpoint(site, session=None):
    endpoint = session.query(Endpoint).filter(Endpoint.site_name == site, Endpoint.in_use == False).first()
    endpoint.update({
        "in_use": True
    })
    return endpoint

def free_endpoint(hostname, session=None):
    endpoint = session.query(Endpoint).filter(Endpoint.hostname == hostname, Endpoint.in_use == True).first()
    endpoint.update({
        "in_use": False
    })
    return

def check_endpoint_truly_in_use(endpoint, session=None):
    return session.query(Request).filter(or_(Request.src_url == endpoint.hostname, Request.dst_url == endpoint.hostname)).first()

# Mesh
def get_vlan_range(site_1, site_2, session=None):
    mesh = session.query(Mesh).filter(Mesh.site_1 == site_1, Mesh.site_2 == site_2).first() or session.query(Mesh).filter(Mesh.site_1 == site_2, Mesh.site_2 == site_1).first()
    vlan_range_start = mesh.vlan_range_start
    vlan_range_end = mesh.vlan_range_end
    if vlan_range_start == -1 or vlan_range_end == -1:
        return "any"
    else:
        return f"{vlan_range_start}-{vlan_range_end}"

def get_max_bandwidth(site, session=None):
    mesh = session.query(Mesh).filter(or_(Mesh.site_1 == site, Mesh.site_2 == site)).all()
    bandwidths = {m.max_bandwidth for m in mesh}
    return max(bandwidths) 
    