import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import numpy as np
import torch
import logging
import unittest
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType, SubnodeType
from sketchgraphs.data._constraint import DirectionValue, HalfSpaceValue
from src.filters.filter_encodegraphconnections import FilterEncodeGraphConnections

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterEncodeGraphConnections(unittest.TestCase):

    def test_process(self):
        mock_sequence_1 = [
        NodeOp(label=EntityType.Point,),
        NodeOp(label=EntityType.Circle,),
        EdgeOp(label=ConstraintType.Radius, references=(1,)),
        NodeOp(label=SubnodeType.SN_Center),
        EdgeOp(label=ConstraintType.Subnode, references=(1,2))]

        filter1 = FilterEncodeGraphConnections(conf_filter={})
        edge_ops = [op for op in mock_sequence_1 if isinstance(op, EdgeOp)]
        node_ops = [op for op in mock_sequence_1 if isinstance(op, NodeOp)]
        message = {'sequence': mock_sequence_1, 'node_ops': node_ops, 'edge_ops': edge_ops}
        filter1.process(message)
        
        # check that 'incidences' contains all edges
        np.testing.assert_allclose(message['incidences'], [[1,1],[1,2]])

        # subnodes
        np.testing.assert_allclose(message['i_edges_given'], [1])
        
        # other edges
        np.testing.assert_allclose(message['i_edges_possible'], [0])

        # edges not in the graph
        torch.testing.assert_allclose(message['edges_toInf_neg'],
            [[0,0], [0,1], [0,2], [2,2]])