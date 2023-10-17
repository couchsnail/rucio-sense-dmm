import json

from dmm.db.models import Request

def subnet_allocation(req, session=None):
    with open("/opt/dmm/sites.json") as f:
        sites = json.load(f)

    src_site = sites.get(req.src_site, {})
    dst_site = sites.get(req.dst_site, {})

    src_ip_block = "best_effort"
    dst_ip_block = "best_effort"

    if req.priority != 0:
        src_allocated = {str(req.src_ipv6_block) for req in session.query(Request.src_ipv6_block).filter(Request.src_site == req.src_site).all()}
        dst_allocated = {str(req.dst_ipv6_block) for req in session.query(Request.dst_ipv6_block).filter(Request.dst_site == req.dst_site).all()}
        for ip_block, url in src_site.get("ipv6_pool", {}).items():
            if ip_block not in src_allocated:
                src_ip_block = ip_block
                break
        for ip_block, url in dst_site.get("ipv6_pool", {}).items():
            if ip_block not in dst_allocated:
                dst_ip_block = ip_block
                break
        src_url = src_site.get("ipv6_pool", {}).get(src_ip_block, "")
        dst_url = dst_site.get("ipv6_pool", {}).get(dst_ip_block, "")

    else:
        src_url = src_site.get("best_effort", "")
        dst_url = dst_site.get("best_effort", "")

    req.update({
        "src_ipv6_block": src_ip_block,
        "dst_ipv6_block": dst_ip_block,
        "src_url": src_url,
        "dst_url": dst_url,
        "transfer_status": "ALLOCATED"
    })

def mark_as_best_effort(req):
    with open("/opt/dmm/sites.json") as f:
        sites = json.load(f)

    src_site = sites.get(req.src_site, {})
    dst_site = sites.get(req.dst_site, {})

    src_ip_block = "best_effort"
    dst_ip_block = "best_effort"

    src_url = src_site.get("best_effort", "")
    dst_url = dst_site.get("best_effort", "")

    req.update({
        "src_ipv6_block": src_ip_block,
        "dst_ipv6_block": dst_ip_block,
        "src_url": src_url,
        "dst_url": dst_url
    })

def stage_sense_link(req, session):
    if req.src_ipv6_block != "best_effort":
        sense_link_id, _ = sense_api.stage_link(
            get_site(req.src_site, session=session).sense_uri,
            get_site(req.dst_site, session=session).sense_uri,
            req.src_ipv6_block,
            req.dst_ipv6_block,
            instance_uuid="",
            alias=req.request_id
        )
        req.update({"sense_link_id": sense_link_id})