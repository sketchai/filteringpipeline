from typing import Dict
import logging

from src.filters.utils.filter_functiononparam import FilterFunctionOnParam
from filtering_pipeline import KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class MockFunctionOnParam(FilterFunctionOnParam):
    """
        A Mock Filter Function On Param.
        Count the number of time apply_function is called.
    """

    def apply_function(self, message, additional_parameters):
        if 'operation_found' in message:
            message['operation_found'] += 1
        else:
            message['operation_found'] = 0
        return message
