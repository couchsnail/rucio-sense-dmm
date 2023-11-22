import networkx as nx

from dmm.db.session import databased
from dmm.utils.db import get_request_by_status, get_site

class NetworkGraph:
    def __init__(self):
        self.graph = nx.MultiGraph()
    
    @databased
    def update(self, session=None):
        reqs_deleted = get_request_by_status(status=["DELETED"], session=session)
        for req_del in reqs_deleted:
            for u, v, key, attr in self.graph.edges(keys=True, data=True):
                if (attr["request_id"] == req_del.request_id):
                    self.graph.remove_edge(req_del.src_site, req_del.dst_site, key=key)
                    break

        reqs =  get_request_by_status(status=["ALLOCATED", "STAGED", "DECIDED", "PROVISIONED", "FINISHED", "STALE"], session=session)
        for req in reqs:
            if req.priority != 0:    
                if not self.graph.has_node(req.src_site):
                    self.graph.add_node(req.src_site, uplink_capacity=get_site(req.src_site, session=session).port_capacity)
                if not self.graph.has_node(req.dst_site):
                    self.graph.add_node(req.dst_site, uplink_capacity=get_site(req.dst_site, session=session).port_capacity)
                if not any(attr["request_id"] == req.request_id for u, v, attr in self.graph.edges(data=True)):
                    self.graph.add_edge(req.src_site, req.dst_site, request_id=req.request_id, priority=req.priority, bandwidth=req.bandwidth)
        
        for src, dst, key, data in self.graph.edges(data=True, keys=True):
            src_capacity = self.graph.nodes[src]["uplink_capacity"]
            dst_capacity = self.graph.nodes[dst]["uplink_capacity"]
            priority = data["priority"]
            
            min_capacity = min(src_capacity, dst_capacity)
            total_priority = sum(edge_data["priority"] for edge_data in self.graph[src][dst].values())
            
            if total_priority == 0:
                updated_bandwidth = 0.0 
            else:
                updated_bandwidth = (min_capacity / total_priority) * priority
                
            self.graph[src][dst][key]["bandwidth"] = round(updated_bandwidth)

        for node in self.graph.nodes:
            total_outgoing_bandwidth = sum(data["bandwidth"] for _, _, data in self.graph.edges(node, data=True))
            uplink_capacity = self.graph.nodes[node]["uplink_capacity"]
            
            if total_outgoing_bandwidth > uplink_capacity:
                scaling_factor = uplink_capacity / total_outgoing_bandwidth
                for _, _, data in self.graph.edges(node, data=True):
                    data["bandwidth"] *= scaling_factor

    def get_bandwidth_for_request_id(self, request_id):
        for source, target, key, data in self.graph.edges(keys=True, data=True):
            if "request_id" in data and data["request_id"] == request_id:
                return int(data["bandwidth"])