from typing import Dict
import re
import logging
import numpy as np

from src.filters.utils.filter_collectparamvalue import FilterCollectParamValue

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterBarycenter(FilterCollectParamValue):
    """
        A filter that recenter the given parameters around the mean

        request : {(type, label) : parameter_name}
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterBarycenter'

    def last_process(self, message: object) -> object:
        all_values = []
        for key, values in self.values.items():
            all_values.extend(values)
        mean = np.mean(all_values)

        for key, reference_list in self.references.items():
            for reference in reference_list:
                reference[key] -= mean

        return message
