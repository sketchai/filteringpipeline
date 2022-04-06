import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import logging
import unittest
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType, SubnodeType
from src.filters.filter_encodeorder import FilterEncodeOrder
from src import OPS_ENCODING_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterEncodeOrder(unittest.TestCase):

    def test_process(self):
        l_keep_edge = [ConstraintType.Coincident, ConstraintType.Distance]
        l_keep_node = [EntityType.Point, EntityType.Circle,
               EntityType.Arc]

        mock_sequence_1 = [
            NodeOp(label=0),
            EdgeOp(label=3, references=(0,)),
            NodeOp(label=2),
        ]
        conf_dict = {'l_keep_node': l_keep_node, 'l_keep_edge': l_keep_edge}
        filter1 = FilterEncodeOrder(conf_filter=conf_dict)
        message = {'sequence': mock_sequence_1}
        filter1.process(message)
        
        edge_offset = len(l_keep_node) + 1
        ref_offset = len(l_keep_node) + 1 + len(l_keep_edge)
        expected_encoding = [
            filter1.node_idx_map[0],
            filter1.edge_idx_map[3]+edge_offset,
            0+ref_offset,
            filter1.node_idx_map[2],
        ]
        self.assertListEqual(message[OPS_ENCODING_TAG], expected_encoding)