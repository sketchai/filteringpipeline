from typing import Dict
import logging
from filtering_pipeline.filters.abstract_filter import AbstractFilter
from filtering_pipeline import KO_FILTER_TAG

from sketchgraphs.data.sequence import EdgeOp


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterConstraintRefs(AbstractFilter):
    """
        A filter that check that no more than x references are used
        if the operation is a constraint

        conf_filter arguments :

            max_refs : int, if n_refs > max_refs send KO
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.max_refs = conf_filter.get('max_refs')
        self.name = 'FilterConstraintRefs'

    def process(self, message: object) -> object:
        op = message.get('op')
        if isinstance(op, EdgeOp):
            if len(op.references) > self.max_refs:
                message.update({KO_FILTER_TAG: self.name})
        return message
