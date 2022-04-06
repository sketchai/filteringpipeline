import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp
from src.filters.filter_constraintrefs import FilterConstraintRefs
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterConstraintRefs(unittest.TestCase):

    def test_refs(self):
        # the max number of refs is inclusive
        filter_conf = {'max_refs': 2}
        filter1 = FilterConstraintRefs(conf_filter=filter_conf)

        # n <= max refs, should not send KO
        message = {'op': EdgeOp(label=0, references=(0, 1))}
        answer = filter1.process(message)
        self.assertIsNone(answer.get(KO_FILTER_TAG))

        # n > max refs, should send KO
        message = {'op': EdgeOp(label=0, references=(0, 1, 2))}
        answer = filter1.process(message)
        tag = answer.get(KO_FILTER_TAG)
        self.assertEqual(tag, filter1.name)

        # Same tests with different label to be sure

        # n <= max refs, should not send KO
        message = {'op': EdgeOp(label=1, references=(0, 1))}
        answer = filter1.process(message)
        self.assertIsNone(answer.get(KO_FILTER_TAG))

        # n > max refs, should send KO
        message = {'op': EdgeOp(label=12, references=(0, 1, 2))}
        answer = filter1.process(message)
        tag = answer.get(KO_FILTER_TAG)
        self.assertEqual(tag, filter1.name)
