from typing import Dict
import re
import logging

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from filtering_pipeline import KO_FILTER_TAG

from src import TYPE_OF_OP_FROM_NAME


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterFunctionOnParam(AbstractFilter):
    """
        A filter that checks the operation and applies a function if it corresponds to the request (a Dict) :
            (type, label): a python object

        The python object associated with each key (type, label) will be used in the apply_function for any purpose.

        The filter is applied on 'op'.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__(conf)
        self.name = 'FilterFunctionOnParam'
        self.request = conf.get('request')

    @staticmethod
    def _check_couple(op, requested_type, requested_label) -> bool:
        """
        Test if the op type and label corresponds to the one tested by the filter
        """
        # Test the type of op
        type_of_op = TYPE_OF_OP_FROM_NAME.get(requested_type)
        test_on_type = isinstance(op, type_of_op)

        # Test the label
        test_on_label = op.label == requested_label

        return test_on_type and test_on_label

    def process(self, message: object) -> object:
        """
        If the operation corresponds to one of the requested couple (type, label), apply function self.apply_function()
        """
        op = message.get('op')
        for couple, additional_parameters in self.request.items():
            if self._check_couple(op, requested_type=couple[0], requested_label=couple[1]):
                message = self.apply_function(message, additional_parameters)
        return message

    def apply_function(self, message, additional_parameters):
        raise NotImplementedError()
