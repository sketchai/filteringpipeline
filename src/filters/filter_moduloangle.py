from typing import Dict
import numpy as np

from src.filters.utils.filter_functiononparam import FilterFunctionOnParam


class FilterModuloAngle(FilterFunctionOnParam):
    """
        A filter that transposes radiant angles in [0;2pi].
    """

    def __init__(self, conf_filter: Dict = {}):
        self.name = 'FilterModuloAngle'
        super().__init__(conf_filter)

    def apply_function(self, message: object, additional_parameters) -> object:
        op = message.get('op')
        if isinstance(additional_parameters, str):
            list_of_params = [additional_parameters]
        else:
            list_of_params = additional_parameters
        
        for parameter_name in list_of_params:
            op.parameters[parameter_name] %= 2*np.pi
        return message

