from dmm.db.session import databased
from dmm.utils.config import config_get
from dmm.utils.db import update_site

@databased
def refresh_site_db(certs=None, session=None):
    sites = config_get("sites", "sites", default=None)
    if sites is None:
        raise IndexError("No sites found in DMM config")
    for site in sites.split(","):
        update_site(site, certs=certs, session=session)
        
@databased
def free_unused_endpoints(session=None):
    # check if any endpoints are in use in db but not really, mark them as not in use
    ...