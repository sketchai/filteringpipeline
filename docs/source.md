# Source 

The source is a simple algorithm that does not have an input port but one output port. For instance, it can generates output based on a file.

*For simple pipeline implementation example, see [test_source](tests/unit/filters/test_source.py).*

## Source object

Source object herits from class [SourceFilter](src/filters/abstract_filter.py).

## SourceFilter 

Object ```SourceFilter``` is an ```AbstractFilter``` object whose abstract method ```process()``` has been implemented.

## How to construct a Source object?

*Example: see [SourceList](src/filters/catalog_source/source_list.py)*

