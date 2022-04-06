from typing import Dict
import logging
from collections import defaultdict

from src.filters.utils.filter_functiononparam import FilterFunctionOnParam

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterCollectParamValue(FilterFunctionOnParam):
    """
        A filter that accesses the value of some parameter and stores its location for later
        (e.g. the length of Lines and the radius of Circles)

        conf_filter:
            request:
                (node, EntityType.Line): 'length'
    """

    def __init__(self, conf: Dict = {}):
        super().__init__(conf)
        self.name = 'SaveParamValue'
        self.values = defaultdict(list)
        self.references = defaultdict(list)

    def apply_function(self, message: Dict, additional_parameters: Dict) -> Dict:
        """
        This function saves the parameters values and references into a dict
        """
        op = message.get('op')
        if isinstance(additional_parameters, str):
            additional_parameters = [additional_parameters]
        for param in additional_parameters:
            param_value = op.parameters.get(param)
            self.values[param].append(param_value)
            self.references[param].append(op.parameters)
        return message
