import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType, ConstraintType
from src.filters.filter_recenterline import FilterRecenterLine
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterRecenterLine(unittest.TestCase):

    def test_process(self):
        op = NodeOp(label=EntityType.Line, parameters={
            'pntX': 1,
            'pntY': 1.,
            'startParam': -2**0.5 / 2,
            'endParam': 2**0.5 / 2,
            'dirX': 2**0.5,
            'dirY': 2**0.5})

        filter1 = FilterRecenterLine()
        message = {'op': op}
        message = filter1.process(message)

        expected_params = {
            'pntX': 0,
            'pntY': 0,
            'startParam': 0,
            'endParam': 2**0.5,
            'dirX': 2**0.5,
            'dirY': 2**0.5
        }

        op = message.get('op')
        for param, expected_value in expected_params.items():
            value = op.parameters.get(param)
            logger.debug(f'testing {param}')
            self.assertAlmostEqual(value, expected_value)
