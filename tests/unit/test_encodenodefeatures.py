import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import torch
import logging
import unittest
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType, SubnodeType
from src.filters.filter_encodenodefeatures import FilterEncodeNodeFeatures

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterEncodeNodeFeatures(unittest.TestCase):

    def test_process(self):
        n_bins = 50
        lMax = 4
        l_keep_node = [EntityType.Point, EntityType.Line, EntityType.Circle, EntityType.Arc]
        
        node_op_0 = NodeOp(label=EntityType.Point, parameters={'isConstruction':0, 'x':0.,'y':1.})
        edge_op_1 = EdgeOp(label=3, references=(0,))
        node_op_2 = NodeOp(label=EntityType.Line, parameters={'isConstruction': 0,'pntX': 0,
                'pntY': 0., 'startParam': 0, 'endParam': 1, 'dirX': 0, 'dirY': 1})
        mock_sequence_1 = [node_op_0, edge_op_1, node_op_2]

        conf_dict = {'l_keep_node': l_keep_node, 'n_bins': n_bins, 'lMax': lMax}
        filter1 = FilterEncodeNodeFeatures(conf_filter=conf_dict)
        message = {'sequence': mock_sequence_1}
        message = filter1.process(message)
        
        # check that 'node_ops' is padded with 'void' nodes
        self.assertListEqual(message['node_ops'],[node_op_0, node_op_2, NodeOp(label='void'), NodeOp(label='void')])

        # check that the label is encoded with ints. 'void' is encoded by len(l_keep_node)
        torch.testing.assert_allclose(message['node_features'], torch.tensor([0, 1, len(l_keep_node), len(l_keep_node)]))

        # distance parameters are encoded from [-1,1] (float) to [0,n_bins-1] (int):
        # -1. -> 0
        #  0. -> n_bins // 2
        #  1. -> n_bins - 1
        expected_result = {
            EntityType.Point : { 'index': torch.tensor([0]), 'value': torch.tensor([[0, n_bins//2, n_bins-1]])},
            EntityType.Line: {'index': torch.tensor([1]), 'value': torch.tensor([[0, n_bins//2, n_bins-1, n_bins//2, n_bins//2, n_bins//2, n_bins-1]])},
            EntityType.Circle: {'index': torch.tensor([], dtype=torch.int64), 'value': torch.zeros((0,7), dtype=torch.int64)},
            EntityType.Arc: {'index': torch.tensor([], dtype=torch.int64), 'value': torch.zeros((0,9), dtype=torch.int64)},
            }
        for key, subdict in message['sparse_node_features'].items():
            index, value = subdict.values()
            exp_index, exp_value = expected_result[key].values()
            torch.testing.assert_allclose(index, exp_index)
            torch.testing.assert_allclose(value, exp_value)

        # check mask with len(node_ops) = 2
        expected_mask = [False]*2 + [True]*(lMax-2)
        torch.testing.assert_allclose(message['mask_attention'], expected_mask)