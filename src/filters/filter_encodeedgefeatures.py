from typing import Dict
import logging
import torch

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from sketchgraphs.data.sequence import EdgeOp
from src.utils.maps import construct_edge_map, construct_edge_map
from src.utils import discretization

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class FilterEncodeEdgeFeatures(AbstractFilter):
    """
        A filter that encodes the features of all edges
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterEncodeEdgeFeatures'
        n_bins = conf_filter.get('n_bins', 50)
        l_keep_edge = conf_filter['l_keep_edge']
        self.edge_idx_map = construct_edge_map(l_keep_edge)
        self.params_edge = discretization.create_params_edge(n_bins)

    def process(self, message: object) -> object:
        sequence = message.get('sequence')
        edge_ops = [op for op in sequence if isinstance(op, EdgeOp)]
        edge_features = torch.tensor([self.edge_idx_map[op.label] for op in edge_ops], dtype=torch.int64)
        sparse_edge_features = discretization.discretization_edges(edge_ops, self.params_edge)
        message['edge_ops'] = edge_ops
        message['edge_features'] = edge_features
        message['sparse_edge_features'] = sparse_edge_features
        return message