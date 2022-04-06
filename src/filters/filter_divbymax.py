from typing import Dict
import re
import logging
import numpy as np

from src.filters.utils.filter_collectparamvalue import FilterCollectParamValue

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterDivByMax(FilterCollectParamValue):
    """
        A filter that divides all collected parameters by the max absolute value

        request : {(type, label) : parameter_name}
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterDivByMax'

    def last_process(self, message: object) -> object:
        max_value = 0

        if self.values.items() is None:
            return message

        for key, values in self.values.items():
            max_value = max(max_value, max(np.abs(values)))

        if max_value == 0:
            return message

        for key, reference_list in self.references.items():
            for reference in reference_list:
                reference[key] /= max_value

        return message
