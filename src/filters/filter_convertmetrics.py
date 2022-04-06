from typing import Dict
import re
import logging

from src.filters.utils.filter_functiononparam import FilterFunctionOnParam
from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp, NodeOp

# a regex that should match all numbers in float notation or '' if no match
NUMBERS_REGEX = r'[-+]?(?:\d*\.\d+|\d+)|$'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterConvertMetrics(FilterFunctionOnParam):
    """
        A filter that accesses the value of a str parameter and converts the metrics using a format_dict

        parms:
          request : {(type, label) : {parameter_name : format_dict}}

        where format_dict = {string_regex: float}
    """

    def __init__(self, conf: Dict = {}):
        super().__init__(conf)
        self.name = 'FilterProcessParam'

    def apply_function(self, message: Dict, additional_parameters: Dict) -> Dict:
        """
        This function formats the given parameter using the format_dict
        """
        op = message.get('op')
        for parameter_name, format_dict in additional_parameters.items():
            param_value = op.parameters.get(parameter_name)
            format_is_in_keys = False

            match, unit_format = self._find_match(param_value, format_dict)
            if match == '':
                logger.debug('no number found')
                message.update({KO_FILTER_TAG: self.name})
                return message

            logger.debug(f'found number {match} in {param_value}')
            new_param_value = float(match) * unit_format
            op.parameters.update({parameter_name: new_param_value})

        return message

    @staticmethod
    def _find_match(param_value, format_dict):
        for regex, unit_format in format_dict.items():
            if re.match(regex, param_value):
                format_is_in_keys = True
                logger.debug(f"{regex} correctly matches {param_value}")
                match = re.findall(NUMBERS_REGEX, param_value)[0]
                return match, unit_format
        return '', None
