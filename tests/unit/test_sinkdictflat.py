import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import torch
import numpy as np
from sketchgraphs.data import flat_array
from src.filters.sink_dictflat import SinkDictFlat
import os
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSinkDictFlat(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/test_sink_dictflat_output.npy'
        self.clean = True
        msg1 = {'node_features': torch.tensor([1,2]), 'edge_features': np.array([3]), 'other_key': 1}
        msg2 = {'node_features': torch.tensor([4,5,6]), 'edge_features': np.array([7]), 'other_key': 2}
        msg3 = {'node_features': torch.tensor([8,9]), 'edge_features': np.array([])}
        self.list_of_msgs = [msg1, msg2, msg3]
        self.slice_length = 2
        self.l_keys = ['node_features', 'edge_features']

    def test_process(self):
        sink = SinkDictFlat(conf_filter={
            'output_path': self.output_path,
            'slice_length': self.slice_length,
            'l_keys': self.l_keys,
            'clean_up': False})

        for message in self.list_of_msgs:
            _ = sink.process(message)
        _ = sink.last_process(message={})

        # Check if the saved dicts contain the correct values
        merged_dicts = list(flat_array.load_flat_array(self.output_path))
        while len(merged_dicts):
            expected_msg = self.list_of_msgs.pop(0)
            saved_dict = merged_dicts.pop(0)
            logger.debug(saved_dict)
            for key in self.l_keys:
                np.testing.assert_allclose(expected_msg[key], saved_dict[key])
                self.assertIsNone(saved_dict.get('other_key'))

        sink._clean_up()
        self.assertFalse(os.path.isdir(sink.output_dir))


    def tearDown(self):
        # Clean and remove created file
        if os.path.isfile(self.output_path) and self.clean:
            os.remove(self.output_path)