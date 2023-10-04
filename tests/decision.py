import sys
sys.path.append("..")

import networkx as nx
import matplotlib.pyplot as plt

from itertools import count

from dmm.db.session import databased
from dmm.utils.dbutil import get_request_by_status
from dmm.core.sense_api import get_uplink_capacity

class NetworkGraph:
    @databased
    def __init__(self, session=None):
        self.graph = nx.MultiGraph()

        reqs =  get_request_by_status(status=["INIT", "STAGED", "FINISHED"], session=session)

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

    def draw(self, filename="graph.png", font_size=8, figure_size=(10, 10), dpi=100):
        # Create a layout for the graph using the spring layout algorithm
        pos = nx.spring_layout(self.graph, seed=42)

        # Draw the nodes and labels with smaller font size
        nx.draw(self.graph, pos, with_labels=True, node_size=300, node_color='lightblue', font_size=font_size)
        node_labels = {node: f"{node}\nCapacity: {data['uplink_capacity']}" for node, data in self.graph.nodes(data=True)}
        nx.draw_networkx_labels(self.graph, pos, labels=node_labels, font_size=font_size)

        # Customize edge labels for MultiGraph
        edge_positions = {}
        edge_count = count()  # Create a counter for multiple edges between the same nodes

        for edge in self.graph.edges(keys=True):
            source, target, key = edge
            edge_positions[(source, target, key)] = (
                pos[source][0] + next(edge_count) * 0.05,
                pos[source][1] + next(edge_count) * 0.05
            )

        # # Manually position and draw edge labels for MultiGraph
        # for edge in self.graph.edges(keys=True, data=True):
        #     source, target, key = edge[:3]
        #     data = edge[3]

        #     x, y = edge_positions[(source, target, key)]
        #     edge_label = f"Request ID: {data['request_id']}\nPriority: {data['priority']}\nBandwidth: {data['bandwidth']}"

        #     plt.text(x, y, edge_label, fontsize=font_size, ha='center', va='center', bbox=dict(facecolor='white', alpha=0.7))

        # Save the graph as an image file with adjusted size and dpi
        plt.figure(figsize=figure_size)
        plt.axis('off')
        plt.savefig(filename, bbox_inches='tight', dpi=dpi)
        plt.close()  # Close the figure to release resources


    def get_bandwidth_for_request_id(self, request_id):
        for source, target, key, data in self.graph.edges(keys=True, data=True):
            if "request_id" in data and data["request_id"] == request_id:
                return data["bandwidth"]
            
if __name__ == "__main__":
    g = NetworkGraph()
    g.draw()