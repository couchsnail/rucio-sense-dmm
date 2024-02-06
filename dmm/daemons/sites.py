from dmm.db.session import databased
from dmm.utils.config import config_get
from dmm.utils.db import update_site, get_all_endpoints

from dmm.db.models import Request
from sqlalchemy import or_

@databased
def refresh_site_db(certs=None, session=None):
    sites = config_get("sites", "sites", default=None)
    if sites is None:
        raise IndexError("No sites found in DMM config")
    for site in sites.split(","):
        update_site(site, certs=certs, session=session)
        
@databased
def free_unused_endpoints(session=None):
    endpoints = get_all_endpoints(session=session)
    for endpoint in endpoints:
        truly_in_use = session.query(Request).filter(or_(Request.src_url == endpoint.hostname, Request.dst_url == endpoint.hostname)).first()
        if not (endpoint.in_use and truly_in_use):
            endpoint.update({
                "in_use": False
            })
