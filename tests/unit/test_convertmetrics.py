import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType
from src.filters.filter_convertmetrics import FilterConvertMetrics
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

NB_RGX = r'[-+]?(?:\d*\.\d+|\d+)'


class TestConvertMetrics(unittest.TestCase):

    def test_process(self):

        conf = {
            'request': {
                ('edge', ConstraintType.Distance): {'length': {f'{NB_RGX} METER': 1., f'{NB_RGX} FEET': 3.28084}},
                ('edge', ConstraintType.Length): {'length': {f'{NB_RGX} METER': 1., f'{NB_RGX} FEET': 3.28084}},
                ('edge', ConstraintType.Diameter): {'length': {f'{NB_RGX} METER': 1., f'{NB_RGX} FEET': 3.28084}},
                ('edge', ConstraintType.Radius): {'length': {f'{NB_RGX} METER': 1., f'{NB_RGX} FEET': 3.28084}},
            }
        }

        filter1 = FilterConvertMetrics(conf=conf)

        # A message with a length in the right unit -> convert
        message_2 = {'op': EdgeOp(
            label=ConstraintType.Diameter,
            references=(1,),
            parameters={'length': '0.008 METER'}
        )
        }
        logger.debug('sending msg 2')
        answer = filter1.process(message_2)
        self.assertAlmostEqual(answer['op'].parameters['length'], 0.008)

        # A message with a length in another accepted unit -> convert
        message_3 = {'op': EdgeOp(
            label=ConstraintType.Diameter,
            references=(1,),
            parameters={'length': '1 FEET'}
        )
        }
        logger.debug('sending msg 3')
        answer = filter1.process(message_3)
        self.assertAlmostEqual(answer['op'].parameters['length'], 3.28084)

        # A message with a bad expression -> KO

        message_4 = {'op': EdgeOp(
            label=ConstraintType.Diameter,
            references=(1,),
            parameters={'length': '#a*#b'}
        )
        }
        logger.debug('sending msg 4')
        answer = filter1.process(message_4)
        self.assertIsNotNone(answer.get(KO_FILTER_TAG))
