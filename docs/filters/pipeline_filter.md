# Class PipelineFilter

Implementation example can be found [here](tests/unit/filters/test_pipelineFilter.py).

## Description

[Class PipelineFilter](src/filters/catalog_filter/pipeline_filter.py) is a class that allows to execute a (sub)-pipeline each time ```process``` method is called. Can be used for fine-grained preprocessing.

## Class arguments details

```PipelineFilter``` is a class that herits from [```AbstractFilter```](src/filters/abstract_filter.py). It has a single argument: a Dict ```conf``` that must contains the following keys:
- *conf_filter*: a configuration filter dict with keys `type` and `parms`.
- *catalog_filter*: a catalog Dict (such as for instance [CATALOG_FILTERS](src/filters/__init__.py))