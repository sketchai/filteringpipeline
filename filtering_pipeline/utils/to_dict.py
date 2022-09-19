from typing import Dict
import collections.abc
import yaml


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
