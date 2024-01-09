import logging

from dmm.db.models import Request, Site

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

def get_request_from_id(rule_id, session=None):
    try:
        req = session.query(Request).filter(Request.rule_id == rule_id).first()
        return req if req else None
    except Exception as e:
        logging.error(f"Error getting request from id: {e}")
        raise

def get_request_by_status(status, session=None):
    try:
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