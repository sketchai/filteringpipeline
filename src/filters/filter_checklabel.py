from typing import Dict

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import NodeOp, EdgeOp


class FilterCheckLabel(AbstractFilter):
    """
        A filter that checks if the label contained into a message is ok or not
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.node_label_list = conf_filter.get('node_label_list')
        self.edge_label_list = conf_filter.get('edge_label_list')
        self.name = 'FilterCheckLabel'

    def process(self, message: object) -> object:
        op = message.get('op')
        if isinstance(op, EdgeOp):
            if op.label not in self.edge_label_list:
                message.update({KO_FILTER_TAG: self.name})
        elif isinstance(op, NodeOp):
            if op.label not in self.node_label_list:
                message.update({KO_FILTER_TAG: self.name})
        return message
