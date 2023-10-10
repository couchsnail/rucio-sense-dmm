import sys
sys.path.append("..")

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.textpath as mtextpath
import matplotlib.patches as mpatches
import matplotlib.transforms as mtransforms
import numpy as np

from dmm.db.session import databased
from dmm.utils.dbutil import get_request_by_status
from dmm.core.sense_api import get_uplink_capacity

class NetworkGraph:
    @databased
    def __init__(self, session=None):
        self.graph = nx.MultiGraph()

        reqs =  get_request_by_status(status=["INIT", "STAGED", "PROVISIONED", "FINISHED"], session=session)

        for req in reqs:
            if req.priority != 0:    
                if not self.graph.has_node(req.src_site):
                    self.graph.add_node(req.src_site, uplink_capacity=get_uplink_capacity(req.src_sense_uri))
                if not self.graph.has_node(req.dst_site):
                    self.graph.add_node(req.dst_site, uplink_capacity=get_uplink_capacity(req.dst_sense_uri))
                self.graph.add_edge(req.src_site, req.dst_site, request_id = req.request_id, priority = req.priority, bandwidth = req.bandwidth)
        
        
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

    def draw(self, filename="graph.png"):
    # Create a layout for the nodes (you can change the layout algorithm as needed)
        pos = nx.random_layout(self.graph)
        nx.draw_networkx_nodes(self.graph, pos, node_color='r', node_size=200, alpha=1)
        ax = plt.gca()

        node_labels = {node: f"{node}\nCapacity: {self.graph.nodes[node]['uplink_capacity']}" for node in self.graph.nodes}
        nx.draw_networkx_labels(self.graph, pos, labels=node_labels, font_size=4)

        for e in self.graph.edges:
            arrow = ax.annotate("",
                xy=pos[e[0]], xycoords='data',
                xytext=pos[e[1]], textcoords='data',
                arrowprops=dict(arrowstyle="<-", color="0.5",
                                shrinkA=5, shrinkB=5,
                                patchA=None, patchB=None,
                                connectionstyle="arc3,rad=rrr".replace('rrr',str(0.3*e[2])
                                ),
                                ),
            )

            print(arrow)

        
      
        # for u, v, key in self.graph.edges(keys=True):
        #     edge_data = self.graph.get_edge_data(u, v, key)
        #     label = f"Request ID: {edge_data['request_id']}\nPriority: {edge_data['priority']}\nBandwidth: {edge_data['bandwidth']:.2f}"

        #     ax.annotate(label,
        #                 xy=pos[u], xycoords='data',
        #                 xytext=pos[v], textcoords='data',
        #                 arrowprops=dict(arrowstyle="->", color="0.5",
        #                                 shrinkA=5, shrinkB=5,
        #                                 patchA=None, patchB=None,
        #                                 connectionstyle="arc3,rad=0.3",
        #                                 ),
        #                 fontsize=8, ha='center', va='center', color='black',
        #                 bbox=dict(boxstyle='round,pad=0.2', edgecolor='0.5', facecolor='white')
        #                 )

        plt.axis('off')
        plt.savefig(filename, )

if __name__ == "__main__":
    g = NetworkGraph()
    g.draw()