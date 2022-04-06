import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType
from src.filters.filter_barycenter import FilterBarycenter
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterBarycenter(unittest.TestCase):

    def test_last_process(self):
        sequence_1 = [
            NodeOp(label=EntityType.Point, parameters={'x': 2., 'y': 2.}),
            EdgeOp(label=0, references=(1, 2), parameters={'x': 1000.}),  # wrong type
            NodeOp(label=EntityType.Circle, parameters={'xCenter': 2., 'yCenter': 4.}),
            NodeOp(label=EntityType.Arc, parameters={'xCenter': 4., 'yCenter': 2}),
            NodeOp(label=EntityType.Line, parameters={'pntX': 4, 'pntY': 4}),
        ]

        conf_x = {
            'request': {
                ('node', EntityType.Line): 'pntX',
                ('node', EntityType.Point): 'x',
                ('node', EntityType.Circle): 'xCenter',
                ('node', EntityType.Arc): 'xCenter',
            }
        }
        conf_y = {
            'request': {
                ('node', EntityType.Line): 'pntY',
                ('node', EntityType.Point): 'y',
                ('node', EntityType.Circle): 'yCenter',
                ('node', EntityType.Arc): 'yCenter',
            }
        }

        filterx = FilterBarycenter(conf_filter=conf_x)
        filtery = FilterBarycenter(conf_filter=conf_y)

        for op in sequence_1:
            message = {'op': op}
            message = filterx.process(message)
            message = filtery.process(message)

        message = filterx.last_process(message)
        _ = filtery.last_process(message)

        self.assertDictEqual(sequence_1[0].parameters, {'x': -1., 'y': -1.})
        self.assertDictEqual(sequence_1[2].parameters, {'xCenter': -1., 'yCenter': 1.})
        self.assertDictEqual(sequence_1[3].parameters, {'xCenter': 1., 'yCenter': -1.})
        self.assertDictEqual(sequence_1[4].parameters, {'pntX': 1., 'pntY': 1.})

        logger.debug(sequence_1)
