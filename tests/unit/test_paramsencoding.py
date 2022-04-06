import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import logging
import unittest
import numpy as np
from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType
from src.filters.filter_paramsencoding import FilterParamsEncoding


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestParamsEncoding(unittest.TestCase):

    @classmethod
    def setUp(self):
        NODES_PARAMETRIZED = {
            EntityType.Point: ['isConstruction', 'x', 'y'],
            EntityType.Line: ['isConstruction', 'dirX', 'dirY', 'pntX', 'pntY', 'startParam', 'endParam'],
            EntityType.Circle: ['isConstruction', 'xCenter', 'yCenter', 'xDir', 'yDir', 'radius', 'clockwise'],
            EntityType.Arc: ['isConstruction', 'xCenter', 'yCenter', 'xDir', 'yDir', 'radius', 'startParam', 'endParam', 'clockwise'],
        }

        self.conf_filter = {'nodes_parametrized': NODES_PARAMETRIZED}

    def test_process(self):
        filter1 = FilterParamsEncoding(self.conf_filter)
        message = {'list_of_sequences': [
            [NodeOp(0, parameters={'isConstruction': True, 'x': 0.1, 'y': 0.1}), EdgeOp(8, references=(0,)), ],
            [NodeOp(0, parameters={'isConstruction': False, 'x': 0.3, 'y': 0.6}), EdgeOp(8, references=(0,)), ],
            [NodeOp(0, parameters={'isConstruction': False, 'x': 0.5, 'y': -0.4}), EdgeOp(8, references=(0,)), ],
        ]}

        answer = filter1.process(message)

        result = message['params_array']
        expected = np.array([
            [1., 0., 0.5],
            [0., 0.5, 1.],
            [0., 1., 0.]
        ])

        for value, check in zip(result.flatten(), expected.flatten()):
            self.assertAlmostEqual(value, check)

    def test_encode_sequence(self):
        seq = [
            NodeOp(7),
            NodeOp(0, parameters={'isConstruction': True, 'x': 0.1, 'y': 0.2}),
            EdgeOp(8, references=(0,), parameters={'length': 12}),
            NodeOp(1, parameters={'isConstruction': False, 'dirX': 0.3, 'dirY': 0.4,
                                  'pntX': 0.5, 'pntY': 0.6, 'startParam': 0., 'endParam': 0.7}),
        ]
        l_params = 10
        filter1 = FilterParamsEncoding(self.conf_filter)
        encoding = filter1._encode_sequence(seq, l_params)

        expected = [1., 0.1, 0.2, 0., 0.3, 0.4, 0.5, 0.6, 0., 0.7]
        for value, check in zip(encoding, expected):
            self.assertAlmostEqual(value, check)

    def test_normalize(self):
        array = np.array([
            [0.00, 0.6, 0.],
            [0.02, 0.3, 0.],
            [0.10, 0.1, 0.],
        ])

        result = FilterParamsEncoding._normalize(array)
        logger.debug(result)

        expected = np.array([
            [0.0, 1.0, 0.],
            [0.2, 0.4, 0.],
            [1.0, 0.0, 0.],
        ])

        for value, check in zip(result.flatten(), expected.flatten()):
            self.assertAlmostEqual(value, check)
