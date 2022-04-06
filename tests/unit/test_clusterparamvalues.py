import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType, ConstraintType
from src.filters.filter_clusterparamvalues import FilterClusterParamValues
import unittest
import logging
import numpy as np

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterClusterParamValues(unittest.TestCase):

    def test_process(self):
        array = np.array([
            [1., 0., 0.5],
            [0., 0.5, 1.],
            [0., 1., 0.]
        ])
        message = {
            'list_of_sequences': [
                [NodeOp(0, parameters={'isConstruction': True, 'x': 0.1, 'y': 0.1}), EdgeOp(8, references=(0,)), ],
                [NodeOp(0, parameters={'isConstruction': False, 'x': 0.3, 'y': 0.6}), EdgeOp(8, references=(0,)), ],
                [NodeOp(0, parameters={'isConstruction': False, 'x': 0.5, 'y': -0.4}), EdgeOp(8, references=(0,)), ],
            ],
            'params_array': array,
        }

        filter1 = FilterClusterParamValues()
        result = filter1.process(message)
        weights = result['weights']
        self.assertAlmostEqual(sum(weights), 1.)
        self.assertEqual(len(weights), 3)
        logger.debug(weights) # should return [0.5, 0.25, 0.25]