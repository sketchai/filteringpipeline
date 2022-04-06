import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import unittest
import logging
import numpy as np

from src.filters.filter_moduloangle import FilterModuloAngle
from sketchgraphs.data.sequence import EdgeOp, NodeOp, EntityType, ConstraintType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterModuloAngle(unittest.TestCase):
    
    def test_process(self):

        conf_filter = {'request':
            {
                ('node', EntityType.Arc): ["startParam","endParam"],
                ('edge', ConstraintType.Angle): "angle"
            }
        }
        filter1 = FilterModuloAngle(conf_filter)


        # testing arc node
        arc = NodeOp(label=EntityType.Arc, parameters={
            'startParam': 38.2*np.pi,
            'endParam': 38.5*np.pi,
        })

        message = filter1.process({'op': arc})
        
        op = message.get('op')
        self.assertAlmostEqual(op.parameters.get('startParam'), 0.2*np.pi)
        self.assertAlmostEqual(op.parameters.get('endParam'), 0.5*np.pi)

        # testing angle constraint
        angle = EdgeOp(label=ConstraintType.Angle, references= None, parameters={'angle': 51*np.pi})
        message = filter1.process({'op': angle})

        op = message.get('op')
        self.assertAlmostEqual(op.parameters.get('angle'), np.pi)
