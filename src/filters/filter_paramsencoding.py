from typing import Dict
import logging
import numpy as np

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from sketchgraphs.data.sequence import NodeOp, EdgeOp, ConstraintType, EntityType
from collections import OrderedDict

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterParamsEncoding(AbstractFilter):
    """
        A filter that encodes the parameters of a list of sequences into a 2d-array
        conf : a Dict : { 'node_label': ['param1', 'param2'] }
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.name = 'FilterParamsEncoding'
        self.nodes_parametrized = OrderedDict(conf_filter['nodes_parametrized'])

    def process(self, message: object) -> object:
        list_of_sequences = message.get('list_of_sequences')
        n_sequences = len(list_of_sequences)
        l_params = 0

        # count the nb of param
        template_seq = list_of_sequences[0]
        for op in template_seq:
            if isinstance(op, NodeOp):
                l_params += len(self.nodes_parametrized.get(op.label, []))

        # fill the params array with the values
        params = np.zeros((n_sequences, l_params))
        for i, seq in enumerate(list_of_sequences):
            params[i] = self._encode_sequence(seq, l_params)

        params = self._normalize(params)

        message['params_array'] = params
        return message

    def _encode_sequence(self, seq, l_params):
        encoding = np.zeros((l_params,))
        offset = 0
        for op in seq:
            if isinstance(op, NodeOp):
                list_of_params = self.nodes_parametrized.get(op.label, [])
                for j, parameter_name in enumerate(list_of_params):
                    encoding[offset + j] = float(op.parameters.get(parameter_name))

                offset += len(list_of_params)

        return encoding

    @staticmethod
    def _normalize(array):
        """normalize columns in [0,1]"""
        array -= np.min(array, axis=0, keepdims=True)
        peek_to_peek = np.ptp(array, axis=0, keepdims=True)
        np.divide(array, peek_to_peek, where=peek_to_peek != 0., out=array)
        return array
