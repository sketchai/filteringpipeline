import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from tests.asset.mock.mock_functionOnParam import MockFunctionOnParam
from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp, NodeOp, ConstraintType, EntityType
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterParamFormat(unittest.TestCase):

    @classmethod
    def setUp(self):
        d_requests = {('edge', ConstraintType.Distance): None,
                      ('node', EntityType.Line): None}
        self.filter = MockFunctionOnParam(conf={'request': d_requests})

    def test_check_couple(self):
        op = EdgeOp(label=2, references=(1,))  # Mirror Constraint
        res = self.filter._check_couple(op=op, requested_type='edge', requested_label=ConstraintType.Coincident)
        self.assertFalse(res)

        res = self.filter._check_couple(op=op, requested_type='edge', requested_label=ConstraintType.Mirror)
        self.assertTrue(res)

        op = NodeOp(label=0)  # Point Primitive
        res = self.filter._check_couple(op=op, requested_type='node', requested_label=EntityType.Point)
        self.assertTrue(res)

        res = self.filter._check_couple(op=op, requested_type='node', requested_label=EntityType.Line)
        self.assertFalse(res)

        res = self.filter._check_couple(op=op, requested_type='edge', requested_label=EntityType.Line)
        self.assertFalse(res)

    def test_process(self):
        message = {'op': NodeOp(label=0)}
        message_out = self.filter.process(message)
        self.assertDictEqual(message_out, {'op': NodeOp(label=0)})
        logger.debug(f'message = {message_out}')

        message = {'op': NodeOp(label=1)}
        message_out = self.filter.process(message)
        self.assertDictEqual(message_out, {'op': NodeOp(label=1), 'operation_found': 0})
        logger.debug(f'message = {message_out}')

        message = {'op': EdgeOp(label=2, references=(1,))}
        message_out = self.filter.process(message)
        self.assertDictEqual(message_out, {'op': EdgeOp(label=2, references=(1,))})
        logger.debug(f'message = {message_out}')

        message = {'op': EdgeOp(label=3, references=(1,))}
        message_out = self.filter.process(message)
        self.assertDictEqual(message_out, {'op': EdgeOp(label=3, references=(1,)), 'operation_found': 0})
        logger.debug(f'message = {message_out}')
