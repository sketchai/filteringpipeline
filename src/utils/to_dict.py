from typing import Dict
import collections.abc
import yaml
from sketchgraphs.data.sequence import NodeOp, EdgeOp, EntityType, ConstraintType, SubnodeType

ENTITY_TYPE_MAPPING = {e.name: e for e in EntityType}
CONSTRAINT_TYPE_MAPPING = {c.name: c for c in ConstraintType}
SUBNODE_TYPE_MAPPING = {s.name: s for s in SubnodeType}

yaml.add_constructor(u'!EntityType', (lambda l,n : ENTITY_TYPE_MAPPING[l.construct_scalar(n)]))
yaml.add_constructor(u'!ConstraintType', (lambda l,n : CONSTRAINT_TYPE_MAPPING[l.construct_scalar(n)]))
yaml.add_constructor(u'!SubnodeType', (lambda l,n : SUBNODE_TYPE_MAPPING[l.construct_scalar(n)]))


def yaml_to_dict(file_path: str) -> Dict:
    with open(file_path, "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
        return data
    return None


def update_nested_dict(d: Dict, u: Dict) -> Dict:
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update_nested_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d
