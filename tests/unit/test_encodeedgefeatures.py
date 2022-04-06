import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import torch
import logging
import unittest
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType, SubnodeType
from sketchgraphs.data._constraint import DirectionValue, HalfSpaceValue
from src.filters.filter_encodeedgefeatures import FilterEncodeEdgeFeatures

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterEncodeEdgeFeatures(unittest.TestCase):

    def test_process(self):
        n_bins = 50
        l_keep_edge = [ConstraintType.Horizontal, ConstraintType.Vertical, ConstraintType.Distance]
        
        node_op_0 = NodeOp(label=EntityType.Line,)
        node_op_1 = NodeOp(label=EntityType.Line,)
        edge_op_2 = EdgeOp(label=ConstraintType.Horizontal, references=(0,))
        edge_op_3 = EdgeOp(label=ConstraintType.Distance, references=(0,1), parameters= {
            'direction': 'MINIMUM', 'halfSpace0': 'LEFT', 'halfSpace1': 'RIGHT', 'length': 1.})
        mock_sequence_1 = [node_op_0, node_op_1, edge_op_2, edge_op_3]

        conf_dict = {'l_keep_edge': l_keep_edge, 'n_bins': n_bins}
        filter1 = FilterEncodeEdgeFeatures(conf_filter=conf_dict)
        message = {'sequence': mock_sequence_1}
        filter1.process(message)
        
        # check that 'edge_ops' contains all edges
        self.assertListEqual(message['edge_ops'],[edge_op_2, edge_op_3])

        # check that the label is encoded with ints (according to l_keep_edge)
        logger.debug(filter1.edge_idx_map)
        torch.testing.assert_allclose(message['edge_features'], torch.tensor([0, 2]))

        # the length is encoded from [-1,1] (float) to [0,n_bins-1] (int):
        # -1. -> 0
        #  0. -> n_bins // 2
        #  1. -> n_bins - 1
        # the enum params (DirectionValue and HalfSpaceValue) are encoded as ints too
        angle_params = torch.tensor(
            [[DirectionValue['MINIMUM'], HalfSpaceValue['LEFT'],HalfSpaceValue['RIGHT'], n_bins-1]])
        expected_result = {
            ConstraintType.Distance: { 'index': torch.tensor([1]),
                'value': angle_params},
                
            # the rest are empty tensors
            ConstraintType.Angle: {'index': torch.tensor([], dtype=torch.int64),
                'value': torch.zeros((0,3), dtype=torch.int64)},
            ConstraintType.Length: {'index': torch.tensor([], dtype=torch.int64),
                'value': torch.zeros((0,2), dtype=torch.int64)},
            ConstraintType.Diameter: {'index': torch.tensor([], dtype=torch.int64),
                'value': torch.zeros((0,1), dtype=torch.int64)},
            ConstraintType.Radius: {'index': torch.tensor([], dtype=torch.int64),
                'value': torch.zeros((0,1), dtype=torch.int64)},
            }
        for key, subdict in message['sparse_edge_features'].items():
            index, value = subdict.values()
            exp_index, exp_value = expected_result[key].values()
            logger.debug(f'{(index,value,exp_index,exp_value)}')

            torch.testing.assert_allclose(index, exp_index)
            torch.testing.assert_allclose(value, exp_value)