import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.filters.filter_checklabel import FilterCheckLabel
from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data import sketch as datalib
from sketchgraphs.data.sequence import NodeOp, EdgeOp
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterCheckLabel(unittest.TestCase):

    def test_process(self):
        filter1 = FilterCheckLabel(conf_filter={
            'node_label_list': [datalib.EntityType.Point, datalib.EntityType.Line, datalib.EntityType.Circle],
            'edge_label_list': [datalib.ConstraintType.Coincident, datalib.ConstraintType.Distance, datalib.ConstraintType.Horizontal]
        })

        # Test 1 : the op fulfills the condition
        message_A = {'op': NodeOp(label=0)}
        answer = filter1.process(message_A)
        self.assertIsNone(answer.get(KO_FILTER_TAG))

        message_A = {'op': EdgeOp(label=0, references=(1,))}
        answer = filter1.process(message_A)
        self.assertIsNone(answer.get(KO_FILTER_TAG))

        # Test 2 : the op does not fulfill the condition
        message_A = {'op': NodeOp(label=12)}
        answer = filter1.process(message_A)
        tag = answer.get(KO_FILTER_TAG)
        self.assertEqual(tag, filter1.name)

        message_A = {'op': EdgeOp(label=12, references=(1,))}
        answer = filter1.process(message_A)
        tag = answer.get(KO_FILTER_TAG)
        self.assertEqual(tag, filter1.name)
