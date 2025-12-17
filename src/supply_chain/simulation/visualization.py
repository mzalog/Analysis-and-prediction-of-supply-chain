import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from typing import List
from .schema import Event, EventType, NodeType

class SimulationVisualizer:
    def __init__(self, events: List[Event], graph: nx.DiGraph):
        self.events = events
        self.graph = graph

    def plot_graph(self, output_path: str):
        plt.figure(figsize=(12, 8))

        pos = {node_id: (self.graph.nodes[node_id]['data'].lon, self.graph.nodes[node_id]['data'].lat) 
               for node_id in self.graph.nodes()}
        
        node_colors = []
        node_shapes = []
        

        styles = {
            NodeType.WAREHOUSE: {"color": "red", "marker": "s"},
            NodeType.CUSTOMER: {"color": "green", "marker": "o"},
            NodeType.HUB: {"color": "blue", "marker": "D"},
            NodeType.PORT: {"color": "cyan", "marker": "^"},
            NodeType.INSPECTION: {"color": "orange", "marker": "h"}
        }


        nx.draw_networkx_edges(self.graph, pos, width=1.0, alpha=0.5, arrowsize=20)
        

        drawn_types = set()
        

        for node_id in self.graph.nodes():
            node = self.graph.nodes[node_id]['data']
            style = styles.get(node.type, {"color": "gray", "marker": "o"})
            
            nx.draw_networkx_nodes(
                self.graph, pos, 
                nodelist=[node_id], 
                node_size=700, 
                node_color=style["color"], 
                node_shape=style["marker"],
            )
            drawn_types.add(node.type)

        nx.draw_networkx_labels(self.graph, pos)
        
        edge_data = nx.get_edge_attributes(self.graph, 'data')
        formatted_labels = {k: f"{v.distance_km:.0f}km" for k, v in edge_data.items()}
        nx.draw_networkx_edge_labels(self.graph, pos, edge_labels=formatted_labels)
        
        from matplotlib.lines import Line2D
        legend_elements = [Line2D([0], [0], marker=s["marker"], color='w', label=t,
                          markerfacecolor=s["color"], markersize=10) for t, s in styles.items()]
        plt.legend(handles=legend_elements, loc='upper right')

        plt.title("Supply Chain Network Graph")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()

    def plot_event_timeline(self, output_path: str):

        data = []
        for ev in self.events:
            data.append({
                "time": ev.time,
                "truck_id": ev.truck_id,
                "event_type": ev.event_type.value,
                "node_id": ev.node_id
            })
        
        df = pd.DataFrame(data)
        if df.empty: return

        plt.figure(figsize=(12, 6))
        
        truck_ids = df['truck_id'].unique()
        for truck in truck_ids:
            subset = df[df['truck_id'] == truck]
            plt.plot(subset['time'], [truck] * len(subset), 'o-', label=truck, markersize=4)
            
        plt.xlabel("Simulation Time (minutes)")
        plt.ylabel("Truck ID")
        plt.title("Truck Event Timeline")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()

    def export_events_to_csv(self, output_path: str):
        data = []
        for ev in self.events:
            row = {
                "time": ev.time,
                "event_id": ev.event_id,
                "truck_id": ev.truck_id,
                "node_id": ev.node_id,
                "event_type": ev.event_type.value,
            }
            row.update(ev.details)
            data.append(row)
            
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)

    @staticmethod
    def animate_simulation(engine, graph_builder):
        import matplotlib.animation as animation
        from .schema import TruckStatus, NodeType
        from matplotlib.lines import Line2D
        
        fig, ax = plt.subplots(figsize=(14, 9))
        graph = graph_builder.graph
        pos = {node_id: (graph.nodes[node_id]['data'].lon, graph.nodes[node_id]['data'].lat) 
               for node_id in graph.nodes()}
        

        styles = {
            NodeType.WAREHOUSE: {"color": "red", "marker": "s"},
            NodeType.CUSTOMER: {"color": "green", "marker": "o"},
            NodeType.HUB: {"color": "blue", "marker": "D"},
            NodeType.PORT: {"color": "cyan", "marker": "^"},
            NodeType.INSPECTION: {"color": "orange", "marker": "h"}
        }

        nx.draw_networkx_edges(graph, pos, width=1.0, alpha=0.5, arrowsize=20, ax=ax)
        
        for node_id in graph.nodes():
            node = graph.nodes[node_id]['data']
            style = styles.get(node.type, {"color": "gray", "marker": "o"})
            nx.draw_networkx_nodes(
                graph, pos, 
                nodelist=[node_id], 
                node_size=700, 
                node_color=style["color"], 
                node_shape=style["marker"],
                ax=ax
            )
        
        nx.draw_networkx_labels(graph, pos, ax=ax)
        
        edge_data = nx.get_edge_attributes(graph, 'data')
        formatted_labels = {k: f"{v.distance_km:.0f}km" for k, v in edge_data.items()}
        nx.draw_networkx_edge_labels(graph, pos, edge_labels=formatted_labels, ax=ax)
        

        legend_elements = [Line2D([0], [0], marker=s["marker"], color='w', label=t,
                          markerfacecolor=s["color"], markersize=10) for t, s in styles.items()]
        ax.legend(handles=legend_elements, loc='upper right', title="Node Types")

        ax.set_title("Live Supply Chain Simulation")
        ax.axis('off')
        
        truck_scatter = ax.scatter([], [], s=150, zorder=5, edgecolors='black')
        

        truck_labels = []
        order_texts = []
        

        hud_text = ax.text(0.02, 0.98, '', transform=ax.transAxes, verticalalignment='top', 
                           bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        route_lines = []
        

        metrics = {
            "total_orders": 0,
            "delivered_orders": 0,
            "total_delivery_time": 0.0
        }

        def init():
            truck_scatter.set_offsets(pd.DataFrame({'x': [], 'y': []}))
            hud_text.set_text('')
            return truck_scatter, hud_text
            
        def update(frame):
            if engine.event_queue:
                engine.step()
            
            completed = [o for o in engine.orders.values() if o.status == "COMPLETED"]
            metrics["delivered_orders"] = len(completed)
            metrics["total_orders"] = len(engine.orders)
            if completed:
                pass


            for line in route_lines: line.remove()
            route_lines.clear()
            
            for txt in truck_labels: txt.remove()
            truck_labels.clear()
            
            for txt in order_texts: txt.remove()
            order_texts.clear()


            x_data = []
            y_data = []
            colors = []
            labels_info = []
            
            status_colors = {
                TruckStatus.IDLE: 'gray',
                TruckStatus.EN_ROUTE_TO_PICKUP: 'blue',
                TruckStatus.EN_ROUTE_TO_DELIVERY: 'green',
                TruckStatus.RESTING: 'red'
            }
            
            active_trucks = 0
            
            for truck_id, truck in engine.trucks.items():
                node_id = truck.current_node_id
                if truck.route and truck.current_node_index < len(truck.route):
                     node_id = truck.route[truck.current_node_index]

                if node_id in pos:
                    x, y = pos[node_id]
                    

                    if truck.current_leg_duration > 0 and truck.route and truck.current_node_index < len(truck.route):
                        
                        start_node_id = truck.current_node_id
                        end_node_id = truck.route[truck.current_node_index]
                        
                        if start_node_id in pos and end_node_id in pos:
                            sx, sy = pos[start_node_id]
                            ex, ey = pos[end_node_id]
                            
                            elapsed = engine.current_time - truck.current_leg_start_time
                            t = elapsed / truck.current_leg_duration
                            t = max(0.0, min(1.0, t))
                            
                            x = sx + (ex - sx) * t
                            y = sy + (ey - sy) * t
                    

                    import random
                    x += random.uniform(-0.01, 0.01)
                    y += random.uniform(-0.01, 0.01)
                    
                    x_data.append(x)
                    y_data.append(y)
                    colors.append(status_colors.get(truck.status, 'black'))
                    
                    order_str = f"({truck.assigned_order_id})" if truck.assigned_order_id else ""
                    labels_info.append((x, y, f"{truck_id}\n{order_str}"))
                    
                    if truck.status != TruckStatus.IDLE:
                        active_trucks += 1


                    if truck.route and truck.current_node_index < len(truck.route):
                        route_x = [x]
                        route_y = [y]
                        for i in range(truck.current_node_index + 1, len(truck.route)):
                            nid = truck.route[i]
                            if nid in pos:
                                route_x.append(pos[nid][0])
                                route_y.append(pos[nid][1])
                        
                        if len(route_x) > 1:
                            line, = ax.plot(route_x, route_y, color=status_colors.get(truck.status, 'black'), 
                                          linestyle='--', alpha=0.5, linewidth=1, zorder=4)
                            route_lines.append(line)

            if x_data:
                truck_scatter.set_offsets(list(zip(x_data, y_data)))
                truck_scatter.set_color(colors)
            

            for x, y, txt in labels_info:
                t = ax.text(x, y+0.04, txt, fontsize=8, ha='center', zorder=6, fontweight='bold')
                truck_labels.append(t)


            pending_counts = {}
            for oid in engine.pending_orders:
                order = engine.orders[oid]
                pending_counts[order.origin_node_id] = pending_counts.get(order.origin_node_id, 0) + 1
            
            for nid, count in pending_counts.items():
                if nid in pos:
                    x, y = pos[nid]
                    # Show a red badge
                    t = ax.text(x+0.05, y+0.05, f"{count}", color='white', fontsize=9, 
                                bbox=dict(boxstyle='circle', facecolor='red', alpha=0.8), zorder=7)
                    order_texts.append(t)


            hud_lines = [
                f"Time: {engine.current_time:.1f} min",
                f"Orders: {metrics['delivered_orders']} / {metrics['total_orders']}",
                f"Queue: {len(engine.event_queue)}",
                f"Active Trucks: {active_trucks} / {len(engine.trucks)}",
                "",
                "Truck Status:"
            ]
            counts = {s: 0 for s in TruckStatus}
            for t in engine.trucks.values(): counts[t.status] += 1
            for s, c in counts.items():
                hud_lines.append(f"  {s.value}: {c}")
                
            hud_text.set_text("\n".join(hud_lines))
            
            return truck_scatter, hud_text
            
        ani = animation.FuncAnimation(fig, update, init_func=init, blit=False, interval=50, cache_frame_data=False)
        plt.show()
