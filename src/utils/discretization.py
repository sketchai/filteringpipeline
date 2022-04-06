import numpy as np
import torch

from sketchgraphs.data import sequence, sketch as datalib
from src.utils.maps import NODES_PARAMETRIZED, EDGES_PARAMETRIZED

def create_params_node(n_bins=50):
    """
    Create dictionaries to discretize all the parameters of the primitives and the constraints. The values of the dictionaries are maps.
    n_bins : int, number of bins to discretize angles, positions and lengths.
    """
    angle_map = np.linspace(0, 2*np.pi, n_bins)  # values have been normalized first
    length_map = np.linspace(-1, 1, n_bins)  # values have been normalized first
    
    params_node = dict([(datalib.EntityType.Point, dict([
                ('isConstruction', datalib.BooleanValue),
                ('x', length_map),
                ('y', length_map)])),
            (datalib.EntityType.Line, dict([
                ('isConstruction', datalib.BooleanValue),
                ('dirX', length_map),
                ('dirY', length_map),
                ('pntX', length_map),
                ('pntY', length_map),
                ('startParam', length_map),
                ('endParam', length_map)])),
            (datalib.EntityType.Circle, dict([
                ('isConstruction', datalib.BooleanValue),
                ('xCenter', length_map),
                ('yCenter', length_map),
                ('xDir', length_map),
                ('yDir', length_map),
                ('radius', length_map),
                ('clockwise', datalib.BooleanValue)])),
           (datalib.EntityType.Arc, dict([
                ('isConstruction', datalib.BooleanValue),
                ('xCenter', length_map),
                ('yCenter', length_map),
                ('xDir', length_map),
                ('yDir', length_map),
                ('radius', length_map),
                ('startParam', angle_map),
                ('endParam', angle_map),
                ('clockwise', datalib.BooleanValue)])) ])
    
    return params_node

def create_params_edge(n_bins=50):
    """
    Create dictionaries to discretize all the parameters of the primitives and the constraints. The values of the dictionaries are maps.
    n_bins : int, number of bins to discretize angles, positions and lengths.
    """
    angle_map = np.linspace(0, 2*np.pi, n_bins)  # values have been normalized first
    length_map = np.linspace(-1, 1, n_bins)  # values have been normalized first

    params_edge = dict([(datalib.ConstraintType.Angle, dict([
                ('aligned', datalib.BooleanValue),
                ('clockwise', datalib.BooleanValue),
                ('angle', angle_map)])),
            (datalib.ConstraintType.Length, dict([
                ('direction', datalib.DirectionValue),
                ('length', length_map)])),
            (datalib.ConstraintType.Distance, dict([
                ('direction', datalib.DirectionValue),
                ('halfSpace0', datalib.HalfSpaceValue),
                ('halfSpace1', datalib.HalfSpaceValue),
                ('length', length_map)])),
            (datalib.ConstraintType.Diameter, dict([('length', length_map)])),
            (datalib.ConstraintType.Radius, dict([('length', length_map)]))])

    return params_edge


def feature_dims(params_edge, params_node):
    """
    Returns the dimensions (i.e. the number of possible discrete values) of the parameters of the primitives and the constraints.
    params_edge, params_node : as returned by the function create_params.
    """
    edge_feature_dimensions = {}
    node_feature_dimensions = {}
    for k, params in params_edge.items():
        edge_feature_dimensions[k] = {}
        for param, map_ in params.items():
            edge_feature_dimensions[k][param] = len(map_)
    for k, params in params_node.items():
        node_feature_dimensions[k] = {}
        for param, map_ in params.items():
            node_feature_dimensions[k][param] = len(map_)
    return edge_feature_dimensions, node_feature_dimensions

def discretization_edges(ops, params_edge):
    """
    Apply the discretization to the constraints.
    ops : list of constraints;
    params_edge : as returned by the function create_params.
    """
    edge_features = {k: {'index': [], 'value':[]} for k in params_edge.keys()}
    
    for i, op in enumerate(ops):
        if op.label not in EDGES_PARAMETRIZED:
            continue
            
        num_feat = []
        for param, map_ in params_edge[op.label].items():
            if isinstance(map_, np.ndarray):
                value = np.searchsorted(map_, op.parameters[param])
            else:
                value = int(map_[op.parameters[param]])
            num_feat.append(value)
            
        edge_features[op.label]['index'].append(i)
        edge_features[op.label]['value'].append(num_feat)
        
    for k in params_edge.keys():
        edge_features[k]['index'] = torch.tensor(edge_features[k]['index'], dtype=torch.int64)
        if edge_features[k]['value']:
            edge_features[k]['value'] = torch.tensor(edge_features[k]['value'], dtype=torch.int64)
        else:
            edge_features[k]['value'] = torch.empty((0, len(params_edge[k])), dtype=torch.int64)
    return edge_features

def discretization_nodes(ops, params_node):
    """
    Apply the discretization to the nodes.
    ops : list of nodes;
    params_node : as returned by the function create_params.
    """
    node_features = {k: {'index': [], 'value':[]} for k in params_node.keys()}
    
    for i, op in enumerate(ops):
        if op.label not in NODES_PARAMETRIZED:
            continue
            
        num_feat = []
        for param, map_ in params_node[op.label].items():
            if isinstance(map_, np.ndarray):
                value = np.searchsorted(map_, op.parameters[param])
            else:
                value = int(map_[op.parameters[param]])
            num_feat.append(value)
            
        node_features[op.label]['index'].append(i)
        node_features[op.label]['value'].append(num_feat)
        
    for k in params_node.keys():
        node_features[k]['index'] = torch.tensor(node_features[k]['index'], dtype=torch.int64)
        if node_features[k]['value']:
            node_features[k]['value'] = torch.tensor(node_features[k]['value'], dtype=torch.int64)
        else:
            node_features[k]['value'] = torch.empty((0, len(params_node[k])), dtype=torch.int64)
    return node_features
    
