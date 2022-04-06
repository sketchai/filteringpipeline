from typing import Dict
import logging
import numpy as np

from sketchgraphs.data import flat_array
from src.filters.sink_slices import SinkSlices

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SinkDictFlat(SinkSlices):
    """
        A sink filter that saves a list of dictionnaries into a flat npy file
    """

    def __init__(self, conf_filter: Dict = {}):
        self.l_keys = conf_filter['l_keys']
        super().__init__(conf_filter)

    def process(self, message: Dict) -> Dict:
        dict_to_save = {}
        for key in self.l_keys:
            dict_to_save[key] = message[key]
        self.collect_data.append(dict_to_save)

        if len(self.collect_data)>=self.slice_length:
            self._save_slice()
        return message