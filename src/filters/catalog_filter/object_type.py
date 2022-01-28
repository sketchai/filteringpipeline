from .abstract_filter import AbstractFilter


class ObjectTypeFilter(AbstractFilter):
    def __init__(self, conf_filter: Dict = {}):
        super(AbstractFilter).__init__(conf_filter)
        self.accepted_object = conf_filter.get('accepted_object')

    def check(self, ob: object):
        return any([isinstance(ob, o) for o in self.accepted_object])
