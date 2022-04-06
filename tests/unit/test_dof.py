import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType, ConstraintType
from src.filters.filter_dof import FilterDof
import unittest
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterDoF(unittest.TestCase):

    def test_process(self):
        filter1 = FilterDof(conf_filter={'max': 6})
        
        # expected_dof = 6
        self.mock_sequence_1 = [
            NodeOp(label=EntityType.Line),
            NodeOp(label=EntityType.Line),
            EdgeOp(label=ConstraintType.Vertical, references=(0,)),
            EdgeOp(label=ConstraintType.Vertical, references=(1,))
        ]
        # n <= max dof, should not send KO
        message = {'sequence': self.mock_sequence_1}
        answer = filter1.process(message)
        tag = answer.get(KO_FILTER_TAG)
        self.assertIsNone(tag)


        # expected_dof = 7
        self.mock_sequence_1 = [
            NodeOp(label=EntityType.Line),
            NodeOp(label=EntityType.Point),
            NodeOp(label=EntityType.Circle),
            EdgeOp(label=ConstraintType.Vertical, references=(0,)),
            EdgeOp(label=ConstraintType.Radius, references=(2,))
        ]
        # n > max dof, should send KO
        message = {'sequence': self.mock_sequence_1}
        answer = filter1.process(message)
        tag = answer.get(KO_FILTER_TAG)
        self.assertEqual(tag, filter1.name)
