from typing import Dict
import logging

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from sketchgraphs.data.sequence import NodeOp, EdgeOp, EntityType, ConstraintType
from src.utils.maps import construct_edge_map, construct_node_map
from src import OPS_ENCODING_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class FilterEncodeOrder(AbstractFilter):
    """
        A filter that encodes the order of the sequence
    """

    def __init__(self, conf_filter: Dict = {}):
        self.name = 'FilterEncodeOrder'
        super().__init__(conf_filter)
        l_keep_node = conf_filter['l_keep_node']
        l_keep_edge = conf_filter['l_keep_edge']
        self.node_idx_map = construct_node_map(l_keep_node)
        self.edge_idx_map = construct_edge_map(l_keep_edge)
        self.edge_idx_offset = len(self.node_idx_map)
        self.reference_idx_offset = len(self.node_idx_map) + len(self.edge_idx_map)

        logger.debug(self.edge_idx_map)
        logger.debug(self.node_idx_map)
    def process(self, message: object) -> object:
        sequence = message.get('sequence')
        encoded_sequence = []
        for op in sequence:
            if isinstance(op, NodeOp): 
                encoded_sequence.append(self.node_idx_map[op.label])
            elif isinstance(op, EdgeOp):
                encoded_sequence.append(self.edge_idx_map[op.label] + self.edge_idx_offset)
                for ref in op.references:
                    encoded_sequence.append(ref + self.reference_idx_offset)
        message[OPS_ENCODING_TAG] = encoded_sequence
        return message