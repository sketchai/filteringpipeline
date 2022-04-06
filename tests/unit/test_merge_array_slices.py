import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import os
import shutil
import tempfile
import logging
import unittest
import numpy as np
import glob
from src.utils import flat_array
from src.utils.slicing import merge_array_slices
from tests import PATH_TO_MINI_SEQUENCE_DATA

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSlicing(unittest.TestCase):

    @staticmethod
    def _create_slice_folder(path_to_slice, n_slices) -> str:
        input_folder_path = tempfile.mkdtemp(dir='tests/asset/')
        for i in range(n_slices):
            slice_i_path = os.path.join(input_folder_path,f'slice_{i}.npy')
            shutil.copyfile(src=path_to_slice, dst=slice_i_path)
        return input_folder_path

    @classmethod
    def setUp(self) -> None:
        path_to_slice = PATH_TO_MINI_SEQUENCE_DATA
        self.n_slices = 3
        self.input_folder_path = self._create_slice_folder(path_to_slice, self.n_slices)
        self.output_path = 'tests/asset/merged_array.npy'
        self.slice_path_list = glob.glob(f'{self.input_folder_path}/*.npy')
        self.clean = True

    def tearDown(self) -> None:
        if self.clean:
            for path in self.slice_path_list:
                if os.path.isfile(path):
                    os.remove(path)
            if os.path.isdir(self.input_folder_path):
                os.rmdir(self.input_folder_path)
            if os.path.isfile(self.output_path):
                os.remove(self.output_path)

    def test_merge_array_slices(self):
        merged_array = merge_array_slices(self.slice_path_list)
        np.save(self.output_path, merged_array)
        final_array = flat_array.load_flat_array(self.output_path)
        slice_array = flat_array.load_dictionary_flat(PATH_TO_MINI_SEQUENCE_DATA)['sequences']
        self.assertEqual(len(final_array), self.n_slices*len(slice_array))
        logging.debug(final_array)
        slice_len = len(slice_array)
        for i in range(self.n_slices):
            for elt_merged, elt_slice in zip(final_array[i*slice_len : (i+1)*slice_len], slice_array):
                logger.debug(elt_merged)
                self.assertEqual(elt_merged, elt_slice)