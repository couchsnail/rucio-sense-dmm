from dmm.db.session import databased
from dmm.db.models import Site, Endpoint, Mesh

from dmm.utils.config import config_get
from dmm.utils.sense import get_site_info, get_list_of_endpoints
from dmm.utils.db import get_site, get_endpoint

import logging
import json
import ipaddress

# refresh the site database, runs once at startup and then as a daemon in order to keep the site database up to date
@databased
def refresh_site_db(session=None):
    # list of sites is defined in config
    sites = config_get("sites", "sites", default=None)
    # make a list of site_objects so we can make a mesh later
    site_objs = []
    if sites is None:
        raise IndexError("No sites found in DMM config")
    for site in sites.split(","):
        try:
            # check if site already exists in the database
            site_exists = get_site(site, session=session)
            if not site_exists:
                # if not, get the site info from sense and add it to the database
                site_info = get_site_info(site)
                site_info = json.loads(site_info)
                sense_uri = site_info["domain_uri"]
                query_url = site_info["domain_url"]
                site_ = Site(name=site, sense_uri=sense_uri, query_url=query_url)
                site_.save(session=session)
                # create mesh of this site with all previously added sites
                for site_obj in site_objs:
                    vlan_range = config_get("vlan-ranges", f"{site_obj.name}-{site}", default="any")
                    if vlan_range == "any":
                        vlan_range = config_get("vlan-ranges", f"{site}-{site_obj.name}", default="any")
                    if vlan_range == "any":
                        logging.debug(f"No vlan range found for {site_obj.name} and {site}, will default to any")
                    if vlan_range == "any":
                        vlan_range_start = -1
                        vlan_range_end = -1
                    else:
                        vlan_range_start = vlan_range.split("-")[0]
                        vlan_range_end = vlan_range.split("-")[1]
                    for peer_point in site_info["peer_points"]:
                        if str(vlan_range_start) in peer_point["peer_vlan_pool"] and str(vlan_range_end) in peer_point["peer_vlan_pool"]:
                            max_bandwidth = int(peer_point["port_capacity"])
                            break
                    else:
                        max_bandwidth = site_info["peer_points"][0]["port_capacity"]
                    mesh = Mesh(site1=site_obj, site2=site_, vlan_range_start=vlan_range_start, vlan_range_end=vlan_range_end, max_bandwidth=max_bandwidth)
                    mesh.save(session=session)
                site_objs.append(site_)
            else:
                site_ = site_exists
            for block, hostname in get_list_of_endpoints(site_.sense_uri).items():
                if get_endpoint(hostname, session=session) is None:
                    new_endpoint = Endpoint(site=site_,
                                            ip_block=ipaddress.IPv6Network(block).compressed,
                                            hostname=hostname,
                                            )
                    new_endpoint.save(session=session)
        except Exception as e:
            logging.error(f"Error occurred in refresh_site_db for site {site}: {str(e)}")