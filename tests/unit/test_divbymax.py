import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline')

import unittest
import logging


from src.filters.filter_divbymax import FilterDivByMax
from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType, ConstraintType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterDivByMax(unittest.TestCase):

    def test_last_process(self):
        sequence_1 = [
            NodeOp(label=EntityType.Point, parameters={'x': 1., 'y': 2.}),
            NodeOp(label=EntityType.Circle, parameters={'xCenter': 3., 'yCenter': 4., 'radius': 5.}),
            EdgeOp(label=ConstraintType.Distance, references=None, parameters={'length': 6.}),
            EdgeOp(label=ConstraintType.Length, references=None, parameters={'length': 6.}),
            EdgeOp(label=ConstraintType.Diameter, references=None, parameters={'length': 6.}),
            EdgeOp(label=ConstraintType.Radius, references=None, parameters={'length': 6.}),
            NodeOp(label=EntityType.Arc, parameters={'xCenter': 7., 'yCenter': -7., 'radius': 7.}),
            NodeOp(label=EntityType.Line, parameters={'pntX': 8., 'pntY': -8., 'startParam': 9., 'endParam': 10.}),
        ]

        conf_filter = {
            'request': {
                ('node', EntityType.Point): ['x', 'y'],
                ('node', EntityType.Line): ['pntX', 'pntY', 'startParam', 'endParam'],
                ('node', EntityType.Circle): ['xCenter', 'yCenter', 'radius'],
                ('node', EntityType.Arc): ['xCenter', 'yCenter', 'radius'],
                ('edge', ConstraintType.Distance): 'length',
                ('edge', ConstraintType.Length): 'length',
                ('edge', ConstraintType.Diameter): 'length',
                ('edge', ConstraintType.Radius): 'length',
            }
        }

        filter1 = FilterDivByMax(conf_filter=conf_filter)

        for op in sequence_1:
            message = {'op': op}
            message = filter1.process(message)

        _ = filter1.last_process(message)

        self.assertDictEqual(sequence_1[0].parameters, {'x': 0.1, 'y': 0.2})
        self.assertDictEqual(sequence_1[1].parameters, {'xCenter': 0.3, 'yCenter': 0.4, 'radius': 0.5})
        self.assertDictEqual(sequence_1[2].parameters, {'length': 0.6})
        self.assertDictEqual(sequence_1[3].parameters, {'length': 0.6})
        self.assertDictEqual(sequence_1[4].parameters, {'length': 0.6})
        self.assertDictEqual(sequence_1[5].parameters, {'length': 0.6})
        self.assertDictEqual(sequence_1[6].parameters, {'xCenter': 0.7, 'yCenter': -0.7, 'radius': 0.7})
        self.assertDictEqual(sequence_1[7].parameters, {'pntX': 0.8, 'pntY': -0.8, 'startParam': 0.9, 'endParam': 1.})
        logger.debug(sequence_1)
