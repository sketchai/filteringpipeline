from .abstract_filter import AbstractFilter


class CountElementFilter(AbstractFilter):
    def __init__(self, conf_filter: Dict = {}):
        super(AbstractFilter).__init__(conf_filter)
        self.is_element = conf_filter.get('is_element')
        self.max_elt = conf_filter.get('max_elt')
        self.min_elt = conf_filter.get('min_elt')

        self.current_count = 0

    def check(self, ob: object):
        if self.is_element(ob):
            self.current_count += 1
        return self.current_count < self.max_elt

    def check_last(self, ob: object):
        if self.is_element(ob):
            self.current_count += 1
        return self.current_count < self.max_elt or self.current_count > self.min_elt
