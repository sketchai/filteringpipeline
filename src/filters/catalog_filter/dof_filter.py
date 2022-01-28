from .abstract_filter import AbstractFilter


class DofFilter(AbstractFilter):
    def __init__(self, conf_filter: Dict = {}):
        super(AbstractFilter).__init__(conf_filter)
        self.dof_max = conf_filter.get('dof_max')

    def check(self, ob: object):
        return np.sum(dof.get_sequence_dof(seq)) < self.dof_max
