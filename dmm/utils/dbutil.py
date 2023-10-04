import logging
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

    req.update({
        "src_ipv6_block": src_ip_block,
        "dst_ipv6_block": dst_ip_block,
        "src_url": src_site.get("ipv6_pool", {}).get(src_ip_block, ""),
        "dst_url": dst_site.get("ipv6_pool", {}).get(dst_ip_block, "")
    })

def get_request_from_id(request_id, session=None):
    return session.query(Request).filter(Request.request_id == request_id).first()

def get_request_by_status(status, session=None):
    return session.query(Request).filter(Request.transfer_status.in_(status)).all()

def get_active_sites(session=None):
    return session.query(Request.src_site.distinct(), Request.dst_site.distinct()).all()

def mark_requests(reqs, status, session=None):
    for req in reqs:
        req.update(
            {
                "transfer_status": status
            }
        )
        logging.debug(f"Marked {req.request_id} as {status}")