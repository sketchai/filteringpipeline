from typing import Dict
import logging
import torch
import numpy as np
from sketchgraphs.data.sequence import ConstraintType
from filtering_pipeline.filters.abstract_filter import AbstractFilter
from src.utils.maps import construct_edge_map, construct_edge_map
from src.utils import discretization

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class FilterEncodeGraphConnections(AbstractFilter):
    """
        A filter that encodes the incidences of all edges
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterEncodeIncidences'

    def process(self, message: object) -> object:
        edge_ops = message.get('edge_ops')
        incidences = torch.tensor([[op.references[0], op.references[-1]] for op in edge_ops], dtype=torch.int64)
        l = len(message['node_ops'])
        i_edges_given = []
        i_edges_possible = []
        edges_exemple = torch.zeros((l, l), dtype=torch.bool)
        for i, op in enumerate(edge_ops):
            edges_exemple[op.references[0], op.references[-1]] = True
            edges_exemple[op.references[-1], op.references[0]] = True
            if op.label == ConstraintType.Subnode:  # à minima
                i_edges_given.append(i)
            else:
                i_edges_possible.append(i)
        i_edges_given = np.array(i_edges_given, dtype=np.int64)
        i_edges_possible = np.array(i_edges_possible, dtype=np.int64)
        edges_toInf_neg = torch.nonzero(torch.triu(~edges_exemple))

        message['incidences']= incidences
        message['i_edges_given']= i_edges_given
        message['i_edges_possible']= i_edges_possible
        message['edges_toInf_neg']= edges_toInf_neg
        return message