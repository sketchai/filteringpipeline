import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from sketchgraphs.data.sequence import NodeOp, EdgeOp
from sketchgraphs.data import flat_array
from src.filters.sink_slices import SinkSlices
import glob
import os
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSinkSlices(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/test_sink_seq_output.npy'
        self.clean = True
        sequence1 = [NodeOp(label=0), EdgeOp(label=2, references=None), NodeOp(label=1)]
        sequence2 = [NodeOp(label=1)]
        sequence3 = [EdgeOp(label=2, references=None)]
        self.list_of_seqs = [sequence1, sequence2, sequence3]
        self.slice_length = 2

    def test_process(self):
        sink = SinkSlices(conf_filter={
            'output_path': self.output_path,
            'slice_length': self.slice_length,
            'clean_up': False})

        for seq in self.list_of_seqs:
            message = {'sequence': seq}
            _ = sink.process(message)
        _ = sink.last_process(message={})

        # Check if the saved sequences are the same
        merged_slices = list(flat_array.load_flat_array(self.output_path))
        slice_path_list = sorted(glob.glob(f'{sink.output_dir}/*.npy'))
        # Loop through the slices
        for slice_path in slice_path_list:
            slice_i = list(flat_array.load_flat_array(slice_path))
            while len(slice_i):
                expected_sequence = self.list_of_seqs.pop(0)
                self.assertEqual(expected_sequence, slice_i.pop(0))
                self.assertEqual(expected_sequence, merged_slices.pop(0))

        sink._clean_up()
        self.assertFalse(os.path.isdir(sink.output_dir))


    def tearDown(self):
        # Clean and remove created file
        if os.path.isfile(self.output_path):
            os.remove(self.output_path)