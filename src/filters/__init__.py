KO_FILTER_TAG = 'KO_FILTER'  # Tag for data that is KO with filter
END_SOURCE_PIPELINE = 'END_SOURCE'  # No more data into source

# BASIC CATALOG FILTERS
from .catalog_source.source_list import SourceList
CATALOG_FILTERS = {'SourceList': SourceList}


# D_FILTERS = {'label': LabelFilter,
#              'count': CountElementFilter,
#              'type': ObjectTypeFilter}
