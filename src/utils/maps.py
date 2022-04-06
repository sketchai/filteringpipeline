from typing import List, Dict
from sketchgraphs.data import sketch as datalib


def construct_edge_map(l_keep_edge: List) -> Dict:
    edge_map = {}
    i = 0
    for t in l_keep_edge:
        edge_map[t] = i
        i += 1
    return edge_map


def construct_node_map(l_keep_node: List) -> Dict:
    node_map = {}
    i = 0
    for t in l_keep_node:
        node_map[t] = i
        i += 1
    node_map['void'] = len(node_map)
    return node_map

NODES_PARAMETRIZED = {
    datalib.EntityType.Point: ['isConstruction', 'x', 'y'],
    datalib.EntityType.Line: ['isConstruction', 'dirX', 'dirY', 'pntX', 'pntY', 'startParam', 'endParam'],
    datalib.EntityType.Circle: ['isConstruction', 'xCenter', 'yCenter', 'xDir', 'yDir', 'radius', 'clockwise'],
    datalib.EntityType.Arc: ['isConstruction', 'xCenter', 'yCenter', 'xDir', 'yDir', 'radius', 'startParam', 'endParam', 'clockwise']
}

EDGES_PARAMETRIZED = {
    datalib.ConstraintType.Angle: ['aligned', 'clockwise', 'angle'],
    datalib.ConstraintType.Length: ['direction', 'length'],
    datalib.ConstraintType.Distance: ['direction', 'halfSpace0', 'halfSpace1', 'length'],
    datalib.ConstraintType.Diameter: ['length'],
    datalib.ConstraintType.Radius: ['length']
}
