import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import NodeOp, EdgeOp
from sketchgraphs.data import flat_array
from src.filters.sink_sequence import SinkSequence
import os
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSinkSequence(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/my_file.npy'
        self.clean = True
        sequence1 = [NodeOp(label=2), EdgeOp(label=2, references=None), NodeOp(label=1)]
        sequence2 = [NodeOp(label=2)]
        sequence3 = [EdgeOp(label=2, references=None)]
        self.list_of_seqs = [sequence1, sequence2, sequence3]

    def test_process(self):
        sink = SinkSequence(conf_filter={'output_path': self.output_path})
        for seq in self.list_of_seqs:
            message = {'sequence': seq}
            _ = sink.process(message)

        self.assertEqual(sink.collect_data, self.list_of_seqs)

    def test_last_process(self):
        sink = SinkSequence(conf_filter={'output_path': self.output_path})
        sink.collect_data = self.list_of_seqs

        _ = sink.last_process(message={})
        saved_sequences = flat_array.load_flat_array(self.output_path)
        for og_seq, saved_seq in zip(self.list_of_seqs, saved_sequences):
            self.assertEqual(og_seq, saved_seq)

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
