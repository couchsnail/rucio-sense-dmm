from dmm.db.session import databased

from dmm.utils.db import get_unused_endpoint, get_request_by_status, mark_requests
    
@databased
def allocator(session=None):
    reqs_init = [req_init for req_init in get_request_by_status(status=["INIT"], session=session)]
    reqs_finished = [req_fin for req_fin in get_request_by_status(status=["FINISHED"], session=session)]
    for new_request in reqs_init:
        for req_fin in reqs_finished:
            if (req_fin.src_site == new_request.src_site and req_fin.dst_site == new_request.dst_site):
                new_request.update({
                    "src_ipv6_block": req_fin.src_ipv6_block,
                    "dst_ipv6_block": req_fin.dst_ipv6_block,
                    "src_url": req_fin.src_url,
                    "dst_url": req_fin.dst_url,
                    "transfer_status": "ALLOCATED"
                })
                mark_requests([req_fin], "DELETED", session)
                reqs_finished.remove(req_fin)
                break

        else:
            src_endpoint = get_unused_endpoint(new_request.src_site, session=session)
            dst_endpoint = get_unused_endpoint(new_request.dst_site, session=session)

            new_request.update({
                "src_ipv6_block": src_endpoint.ip_block,
                "dst_ipv6_block": dst_endpoint.ip_block,
                "src_url": src_endpoint.hostname,
                "dst_url": dst_endpoint.hostname,
                "transfer_status": "ALLOCATED"
            })
