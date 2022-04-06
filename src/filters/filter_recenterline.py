from typing import Dict

from src.filters.utils.filter_functiononparam import FilterFunctionOnParam
from sketchgraphs.data.sequence import EntityType


class FilterRecenterLine(FilterFunctionOnParam):
    """
        A filter that recenters the line nodes. Does not require yml parameters
    """

    def __init__(self, conf_filter: Dict = None):
        self.name = 'FilterRecenterLine'
        assert conf_filter is None, f'{self.name}\'s conf filter should not have parms'
        default_conf_filter = {'request': {('node', EntityType.Line): None}}
        super().__init__(default_conf_filter)

    def apply_function(self, message: object, additional_parameters) -> object:
        op = message.get('op')
        op.parameters['pntX'] += op.parameters['startParam'] * op.parameters['dirX']
        op.parameters['pntY'] += op.parameters['startParam'] * op.parameters['dirY']
        op.parameters['endParam'] -= op.parameters['startParam']
        op.parameters['startParam'] = 0
        return message
