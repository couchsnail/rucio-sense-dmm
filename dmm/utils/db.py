import logging
import requests

from dmm.db.models import Request, Site, Endpoint

def get_site(site_name, attr=None, session=None):
    try:
        if attr:
            query = session.query(Site).filter(Site.name == site_name).first()
            return getattr(query, attr)
        else:
            return session.query(Site).filter(Site.name == site_name).first()
    except Exception as e:
        logging.error(f"Error getting site: {e}")
        raise

def get_unused_endpoint(site, session=None):
    endpoint = session.query(Endpoint).filter(Endpoint.site == site, Endpoint.in_use == False).first()
    endpoint.update({
        "in_use": True
    })
    return endpoint

def get_request_from_id(rule_id, session=None):
    try:
        req = session.query(Request).filter(Request.rule_id == rule_id).first()
        return req if req else None
    except Exception as e:
        logging.error(f"Error getting request from id: {e}")
        raise

def get_request_by_status(status, session=None):
    try:
        if status == "any":
            return session.query(Request).all()
        else:
            return session.query(Request).filter(Request.transfer_status.in_(status)).all()
    except Exception as e:
        logging.error(f"Error getting request by status: {e}")
        raise

def mark_requests(reqs, status, session=None):
    try:
        for req in reqs:
            req.update({
                "transfer_status": status
            })
            logging.debug(f"Marked {req.rule_id} as {status}")
    except Exception as e:
        logging.error(f"Error marking requests: {e}")
        raise

def update_bandwidth(req, bandwidth, session=None):
    try:
        req.update({
            "bandwidth": bandwidth
        })
        logging.debug(f"Updated bandwidth to {bandwidth} for {req.rule_id}")
    except Exception as e:
        logging.error(f"Error updating bandwidth: {e}")
        raise

def update_priority(req, priority, session=None):
    try:
        req.update({
            "priority": priority
        })
        logging.debug(f"Updated priority to {priority} for {req.rule_id}")
    except Exception as e:
        logging.error(f"Error updating bandwidth: {e}")
        raise

def update_site(site, certs, session=None):
    if get_site(site, session=session) is None:
        src_site = Site(name=site)
        src_site.save(session=session)
        url = str(src_site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
        data = requests.get(url, cert=certs, verify=False).json()
        for block, hostname in data[src_site.name]["metadata"]["xrootd"].items():
            new_endpoint = Endpoint(site=src_site.name,
                                    ip_block=block,
                                    hostname=hostname,
                                    in_use=False
                                    )
            new_endpoint.save(session=session)