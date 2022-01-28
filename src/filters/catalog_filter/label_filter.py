from .abstract_filter import AbstractFilter


class LabelFilter(AbstractFilter):
    def __init__(self, conf_filter: Dict = {}):
        super(AbstractFilter).__init__(conf_filter)
        self.accepted_label = conf_filter.get('accepted_label', [])

    def check(self, ob: object):
        """
            his filter checks first if an object has a 'label' attribut. If so, it checks if the label belongs to the ACCEPTED_LABEL list.
        """
        if hasattr(ob, 'label'):
            return label in self.accepted_label
        else:
            return False
