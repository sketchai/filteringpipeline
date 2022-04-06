import sys
import os
import logging

if __name__ == '__main__':
    sys.path.append('src/sketchgraphs/')
    sys.path.append('src/filtering-pipeline/')
    cur_path = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, cur_path + "/..")
    logging.basicConfig(level=logging.WARNING)
else:
    logging.basicConfig(level=logging.DEBUG)

import pickle

from src.utils import discretization, maps
from experiments import L_KEEP_EDGE, L_KEEP_NODE, L_MAX, L_MIN, DOF_MAX, N_BINS, PARAMETERS_PATH

def export_parameters(fo=PARAMETERS_PATH, lMax=L_MAX, lMin=L_MIN, dof_max=DOF_MAX, n_bins=N_BINS):
    """
    Save the preprocessing parameters; they are required for the training.
    """
    params_edge = discretization.create_params_edge(n_bins=n_bins)
    params_node = discretization.create_params_node(n_bins=n_bins)
    edge_feature_dimensions, node_feature_dimensions = discretization.feature_dims(params_edge=params_edge, params_node=params_node)
    node_idx_map = maps.construct_node_map(l_keep_node=L_KEEP_NODE)
    edge_idx_map = maps.construct_edge_map(l_keep_edge=L_KEEP_EDGE)
    with open(fo, 'wb') as f:
        pickle.dump({
            'lMax': lMax,
            'lMin': lMin,
            'dof_max': dof_max,
            'n_bins': n_bins,
            'node_feature_dimensions': node_feature_dimensions,
            'edge_feature_dimensions': edge_feature_dimensions,
            'node_idx_map': node_idx_map,
            'edge_idx_map': edge_idx_map,
            'padding_idx': node_idx_map['void'], 
                     }, f)

def main():
    export_parameters()

if __name__ == '__main__':
    main()