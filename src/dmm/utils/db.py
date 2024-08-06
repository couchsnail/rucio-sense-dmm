import logging
import ipaddress
from sqlalchemy import text, or_

from dmm.db.models import Request, Site, Endpoint, Mesh, Bytes
from dmm.utils.sense import get_allocation, free_allocation

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

def get_endpoints(req, session=None):
    try:
        free_src_ipv6 = get_allocation(req.src_site, req.rule_id)
        free_src_ipv6 = ipaddress.IPv6Network(free_src_ipv6).compressed
        
        free_dst_ipv6 = get_allocation(req.dst_site, req.rule_id)
        free_dst_ipv6 = ipaddress.IPv6Network(free_dst_ipv6).compressed

        src_endpoint = session.query(Endpoint).filter(Endpoint.site_name == req.src_site, Endpoint.ip_block == free_src_ipv6).first()
        dst_endpoint = session.query(Endpoint).filter(Endpoint.site_name == req.dst_site, Endpoint.ip_block == free_dst_ipv6).first()
        
        if src_endpoint is None or dst_endpoint is None:
            raise Exception("Could not find endpoints")        
        
        return src_endpoint, dst_endpoint
    
    except Exception as e:
        free_allocation(req.src_site, req.rule_id)
        free_allocation(req.dst_site, req.rule_id)
        logging.error(e)

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

#Bytes
def get_interval_cursor(session=None):
    return session.execute(text("SELECT * from bytes")).cursor

def check_intervals(volume, session=None):
    cursor = get_interval_cursor

    #Check if nothing is in the database
    query = f"SELECT COUNT(interval_5) FROM bytes WHERE interval_5 IS NOT NULL"
    cursor.execute(query)
    is_null = bool(cursor.fetchone()[0]==0)
    #If nothing is in the database, insert given volume into interval_5
    if is_null:
        session.execute(f"INSERT INTO bytes (interval_5) VALUES ({volume})")
    #If not, move all values over one interval, inserting this volumen into interval_5
    else:
        is_full = session.execute("SELECT COUNT(interval_1) FROM bytes WHERE interval_1 IS NOT NULL")
        is_full = bool(is_full==0)
        if is_full:
            session.execute(f"UPDATE bytes SET interval_1 = NULL")
        for i in range(5,1,-1):
            cursor.execute(f"SELECT interval_{i} FROM bytes")
            old_volume = cursor.fetchone()[0]
            session.execute(f"UPDATE bytes SET interval_{i-1} = :old_volume")
            session.execute(f"UPDATE bytes SET interval_{i} = :volume")


    