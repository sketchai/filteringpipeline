import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType
from src.filters.utils.filter_collectparamvalue import FilterCollectParamValue
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterCollectParamValue(unittest.TestCase):
    def test_process(self):
        conf = {
            'request': {
                ('node', EntityType.Line): 'length',
                ('node', EntityType.Circle): 'radius'
            }
        }
        filter1 = FilterCollectParamValue(conf=conf)
        logger.debug(filter1.request)
        self.mock_sequence_1 = [
            NodeOp(label=0, parameters={'radius': 0}),  # wrong label
            EdgeOp(label=0, references=(1, 2), parameters={'radius': 0}),  # wrong type
            NodeOp(label=2, parameters={'radius': 42}),  # a Circle
            NodeOp(label=3),
            NodeOp(label=1, parameters={'length': 1}),  # a Line
            NodeOp(label=2, parameters={'radius': -42}),  # another Circle
            EdgeOp(label=0, references=(1, 2)),
            NodeOp(label=12)
        ]

        for op in self.mock_sequence_1:
            message = {'op': op}
            filter1.process(message)

        # check that we correctly collected the value

        self.assertEqual(filter1.values['radius'], [42, -42])
        self.assertEqual(filter1.values['length'], [1])

        # check that we can modify the parameters by accessing the reference

        for reference in filter1.references['radius']:
            reference['radius'] /= 2

        self.assertEqual(self.mock_sequence_1[2].parameters['radius'], 21)
        self.assertEqual(self.mock_sequence_1[5].parameters['radius'], -21)

        for reference in filter1.references['length']:
            reference['length'] *= 2

        self.assertEqual(self.mock_sequence_1[4].parameters['length'], 2)

        logger.debug(self.mock_sequence_1)
