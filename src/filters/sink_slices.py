import glob
import tempfile
from typing import Dict
import logging
import numpy as np
import os

from src.utils.slicing import merge_array_slices
from sketchgraphs.data import flat_array
from filtering_pipeline.filters.abstract_filter import AbstractFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SinkSlices(AbstractFilter):
    """
        A sink filter that saves sequences into npy slices then merges them into one big array
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.output_path: str = conf_filter.get('output_path')
        self.slice_length: int = conf_filter.get('slice_length')
        self.clean : bool = conf_filter.get('clean_up', True)

        directory_path = os.path.dirname(os.path.abspath(self.output_path))
        self.output_dir: str = tempfile.mkdtemp(dir=directory_path, prefix='slices')
        self.slice_number = 0
        self.collect_data = []

    def process(self, message: Dict) -> Dict:
        self.collect_data.append(message.get('sequence'))
        if len(self.collect_data)>=self.slice_length:
            self._save_slice()
        return message

    def last_process(self, message: Dict) -> Dict:
        self._save_slice()
        self._merge_all_slices()
        if self.clean:
            self._clean_up()
        return message

    def _save_slice(self):
        data = flat_array.save_list_flat(self.collect_data)
        slice_output_path = os.path.join(self.output_dir, f'slice_{self.slice_number:05d}.npy')
        np.save(slice_output_path, data, allow_pickle=False)
        self.slice_number+=1
        self.collect_data = []

    def _merge_all_slices(self):
        slice_path_list = sorted(glob.glob(f'{self.output_dir}/*.npy'))
        logger.debug(slice_path_list)
        merged_array = merge_array_slices(slice_path_list)
        np.save(self.output_path, merged_array)

    def _clean_up(self):
        # Remove slices
        slice_path_list = glob.glob(f'{self.output_dir}/*.npy')
        for path in slice_path_list:
            if os.path.isfile(path):
                os.remove(path)
        os.rmdir(self.output_dir)