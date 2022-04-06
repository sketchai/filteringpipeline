from typing import Dict
import logging
import torch

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from sketchgraphs.data.sequence import NodeOp
from src.utils.maps import construct_edge_map, construct_node_map
from src.utils import discretization

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class FilterEncodeNodeFeatures(AbstractFilter):
    """
        A filter that encodes the features of all nodes
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterEncodeOrder'
        n_bins = conf_filter.get('n_bins', 50)
        l_keep_node = conf_filter['l_keep_node']
        self.lMax = conf_filter.get('lMax',60)
        self.node_idx_map = construct_node_map(l_keep_node)
        self.params_node = discretization.create_params_node(n_bins)

    def process(self, message: object) -> object:
        sequence = message.get('sequence')
        node_ops = [op for op in sequence if isinstance(op, NodeOp)]
        l = len(node_ops)            
        node_ops += [NodeOp('void')]*(self.lMax-l)
        node_features = torch.tensor([self.node_idx_map[op.label] for op in node_ops], dtype=torch.int64)
        sparse_node_features = discretization.discretization_nodes(node_ops, self.params_node)

        mask_attention = torch.ones(self.lMax, dtype=torch.bool)
        mask_attention[:l] = False
        message['node_ops'] = node_ops
        message['node_features'] = node_features
        message['sparse_node_features'] = sparse_node_features
        message['mask_attention'] = mask_attention
        message['length'] = l
        return message