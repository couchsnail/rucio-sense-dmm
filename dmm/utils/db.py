import logging
import requests

from dmm.db.models import Request, Site
from dmm.utils.config import config_get

def get_site(site_name, session=None):
    return session.query(Site).filter(Site.name == site_name).first()

def get_request_from_id(request_id, session=None):
    return session.query(Request).filter(Request.request_id == request_id).first()

def get_request_by_status(status, session=None):
    return session.query(Request).filter(Request.transfer_status.in_(status)).all()

def get_active_sites(session=None):
    return session.query(Request.src_site.distinct(), Request.dst_site.distinct()).all()

def mark_requests(reqs, status, session=None):
    for req in reqs:
        req.update({
            "transfer_status": status
        })
        logging.debug(f"Marked {req.request_id} as {status}")

def update_bandwidth(req, bandwidth, session=None):
    req.update({
        "bandwidth": bandwidth
    })
    logging.debug(f"Updated bandwidth to {bandwidth} for {req.request_id}")

def get_url_from_block(site, ipv6_block, session=None):
    cert = config_get("dmm", "siterm_cert")
    key = config_get("dmm", "siterm_key")
    capath = "/etc/grid-security/certificates"
    
    site_ = get_site(site, session)
    data = requests.get(str(site_.query_url) + "/MAIN/sitefe/json/frontend/configuration", cert=(cert, key), verify=capath)

    return data["general"]["metadata"]["xrootd"][ipv6_block]