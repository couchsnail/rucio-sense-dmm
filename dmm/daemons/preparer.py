request_id = Request.id(rule_id, src_rse_name, dst_rse_name)
if request_id in self.requests.keys():
    logging.error("request ID already processed, appending to the list of transfers")
    continue
# Retrieve or construct source Site object
src_site = self.sites.get(src_rse_name, Site(src_rse_name))
if src_rse_name not in self.sites.keys():
    self.sites[src_rse_name] = src_site
# Retrieve or construct destination Site object
dst_site = self.sites.get(dst_rse_name, Site(dst_rse_name))
if dst_rse_name not in self.sites.keys():
    self.sites[dst_rse_name] = dst_site
# Create new Request
request = Request(rule_id, src_site, dst_site, **request_attr)
request.register()
# Store new request and its corresponding link
self.requests[request_id] = request 

self.update_requests("accommodating for new requests")