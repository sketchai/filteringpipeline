from typing import Dict
import logging
from filtering_pipeline import KO_FILTER_TAG
from filtering_pipeline.filters.abstract_filter import AbstractFilter
from sketchgraphs.data.dof import get_sequence_dof

class FilterDof(AbstractFilter):
    def __init__(self, conf_filter: Dict):
        self.name = 'FilterDof'
        self.max = conf_filter.get('max')
        super().__init__(conf_filter)

    def process(self, message: Dict) -> Dict:  
        if self.max is None:
            return message
        seq = message['sequence']
        array_dof = get_sequence_dof(seq)
        dof = sum(array_dof)
        if dof>self.max:
            message[KO_FILTER_TAG]=self.name
        return message
