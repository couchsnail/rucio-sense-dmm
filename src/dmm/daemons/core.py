import copy
from networkx import MultiGraph

from dmm.db.session import databased
from dmm.utils.db import get_requests, mark_requests, update_bandwidth, get_site, get_unused_endpoint, get_max_bandwidth

@databased
def decider(session=None):
    network_graph = MultiGraph()
    # Get all active requests
    reqs = get_requests(status=["STAGED", "ALLOCATED", "MODIFIED", "DECIDED", "STALE", "PROVISIONED", "FINISHED", "CANCELED"], session=session)
    for req in reqs:
        src_port_capacity = get_max_bandwidth(req.src_site, session=session)
        network_graph.add_node(req.src_site, port_capacity=src_port_capacity, remaining_capacity=src_port_capacity)
        dst_port_capacity = get_max_bandwidth(req.dst_site, session=session)
        network_graph.add_node(req.dst_site, port_capacity=dst_port_capacity, remaining_capacity=dst_port_capacity)
        network_graph.add_edge(req.src_site, req.dst_site, rule_id=req.rule_id, priority=req.priority, bandwidth=req.bandwidth)
    
    # exit if graph is empty
    if not network_graph.nodes:
        return
    
    # for prio modified reqs, update prio in graph, this is a very bad way of doing things and can be fixed by sharing the network_graph object
    # between processes and update the prio in the graph where I set modified bandwidth, but sharing complex objects between multiprocessing
    # processes is non-trivial
    reqs_modified = [req for req in get_requests(status=["MODIFIED"], session=session)]
    for req in reqs_modified:
        for _, _, key, data in network_graph.edges(keys=True, data=True):
            if "rule_id" in data and data["rule_id"] == req.rule_id:
                data["priority"] = req.modified_priority

    network_graph_copy = copy.deepcopy(network_graph)
    # recursively update the graph, probably garbage scaling but I am assuming this will never be used for more than O(10) nodes.
    #TODO: update this comment to explain how this works.
    while len(network_graph_copy.nodes) > 1:
        total_priority_filter = lambda x : sum(rule['priority'] for rules in network_graph_copy[x].values() for rule in rules.values())
        max_node = sorted(network_graph_copy.nodes, key=total_priority_filter, reverse=True)[0]
        
        network_graph_copy_copy = copy.deepcopy(network_graph_copy)
        for src, dst, key, data in sorted(network_graph_copy_copy.edges(max_node, data=True, keys=True), key=lambda x: network_graph_copy_copy.nodes[x[1]]["remaining_capacity"]):
            total_priority = sum(rule['priority'] for rules in network_graph_copy_copy[max_node].values() for rule in rules.values())

            min_capacity = min(network_graph_copy_copy.nodes[node]["remaining_capacity"] for node in network_graph_copy_copy.nodes)        
            priority = data["priority"]

            updated_bandwidth = (min_capacity / total_priority) * priority
            updated_bandwidth = updated_bandwidth - (updated_bandwidth % 1000)

            network_graph[src][dst][key]["bandwidth"] = updated_bandwidth
            network_graph_copy_copy.nodes[src]["remaining_capacity"] = network_graph_copy_copy.nodes[src]["remaining_capacity"] - updated_bandwidth
            network_graph_copy_copy.nodes[dst]["remaining_capacity"] = network_graph_copy_copy.nodes[dst]["remaining_capacity"] - updated_bandwidth
            network_graph_copy_copy.remove_edge(src, dst, key)
            
            if network_graph_copy_copy.number_of_edges(src, dst) == 0:
                network_graph_copy_copy.remove_node(dst)

        network_graph_copy.remove_node(max_node)

    # for staged reqs, allocate new bandwidth
    reqs_staged = [req for req in get_requests(status=["STAGED"], session=session)]
    for req in reqs_staged:
        for _, _, key, data in network_graph.edges(keys=True, data=True):
            if "rule_id" in data and data["rule_id"] == req.rule_id:
                allocated_bandwidth = int(data["bandwidth"])
        update_bandwidth(req, allocated_bandwidth, session=session)
        mark_requests([req], "STAGED", session)

    # for already provisioned reqs, modify bandwidth and mark as stale
    reqs_provisioned = [req for req in get_requests(status=["MODIFIED", "PROVISIONED"], session=session)]
    for req in reqs_provisioned:
        for _, _, key, data in network_graph.edges(keys=True, data=True):
            if "rule_id" in data and data["rule_id"] == req.rule_id:
                allocated_bandwidth = int(data["bandwidth"])
        if allocated_bandwidth != req.bandwidth:
            update_bandwidth(req, allocated_bandwidth, session=session)
            mark_requests([req], "STALE", session)

@databased
def allocator(session=None):
    reqs_init = [req_init for req_init in get_requests(status=["INIT"], session=session)]
    for new_request in reqs_init:        
        reqs_finished = [req_fin for req_fin in get_requests(status=["FINISHED"], session=session)]
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