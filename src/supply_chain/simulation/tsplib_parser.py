

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple
import math


@dataclass
class TSPNode:

    id: int
    x: float
    y: float


def parse_tsplib(file_path: Path) -> Tuple[str, List[TSPNode]]:
    """
    Parse a TSPLIB format file and return metadata and node list.
    
    Args:
        file_path: Path to the .txt or .tsp file
        
    Returns:
        Tuple of (problem_name, list of TSPNode)
    """
    nodes: List[TSPNode] = []
    name = "unknown"
    in_coord_section = False
    
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            
            if not line or line == "EOF":
                continue
                
            if line.startswith("NAME"):
                # NAME: kroA100 or NAME : kroA100
                name = line.split(":")[-1].strip()
                continue
                
            if line == "NODE_COORD_SECTION":
                in_coord_section = True
                continue
                
            if in_coord_section:

                parts = line.split()
                if len(parts) >= 3:
                    try:
                        node_id = int(parts[0])
                        x = float(parts[1])
                        y = float(parts[2])
                        nodes.append(TSPNode(id=node_id, x=x, y=y))
                    except ValueError:

                        continue
    
    return name, nodes


def euclidean_distance(n1: TSPNode, n2: TSPNode) -> float:

    return math.sqrt((n1.x - n2.x) ** 2 + (n1.y - n2.y) ** 2)


def normalize_coordinates(nodes: List[TSPNode], 
                          lat_range: Tuple[float, float] = (45.0, 55.0),
                          lon_range: Tuple[float, float] = (14.0, 24.0)) -> List[Tuple[float, float]]:
    """
    Normalize TSP coordinates to realistic lat/lon ranges (default: Central Europe).
    
    Args:
        nodes: List of TSPNode with x,y coordinates
        lat_range: Target latitude range (min, max)
        lon_range: Target longitude range (min, max)
        
    Returns:
        List of (lat, lon) tuples in the same order as input nodes
    """
    if not nodes:
        return []
    
    x_vals = [n.x for n in nodes]
    y_vals = [n.y for n in nodes]
    
    x_min, x_max = min(x_vals), max(x_vals)
    y_min, y_max = min(y_vals), max(y_vals)
    
    # Avoid division by zero
    x_span = x_max - x_min if x_max != x_min else 1.0
    y_span = y_max - y_min if y_max != y_min else 1.0
    
    # Calculate spans
    x_span = x_max - x_min if x_max != x_min else 1.0
    y_span = y_max - y_min if y_max != y_min else 1.0
    
    lat_min, lat_max = lat_range
    lon_min, lon_max = lon_range
    
    lat_span_target = lat_max - lat_min
    lon_span_target = lon_max - lon_min
    

    
    avg_lat_rad = math.radians((lat_min + lat_max) / 2)
    lon_correction = math.cos(avg_lat_rad)
    

    
    target_height = lat_span_target
    target_width = lon_span_target * lon_correction
    
    scale_y = target_height / y_span
    scale_x = target_width / x_span
    
    scale = min(scale_x, scale_y)
    
    new_height = y_span * scale
    new_width = x_span * scale
    

    new_width_lon = new_width / lon_correction
    

    lat_center = (lat_min + lat_max) / 2
    lon_center = (lon_min + lon_max) / 2
    
    result = []
    for node in nodes:

        rel_x = node.x - (x_min + x_max) / 2
        rel_y = node.y - (y_min + y_max) / 2
        
        # Scale
        scaled_y = rel_y * scale
        scaled_x = rel_x * scale
        

        d_lat = scaled_y
        d_lon = scaled_x / lon_correction
        
        lat = lat_center + d_lat
        lon = lon_center + d_lon
        
        result.append((lat, lon))
    
    return result
