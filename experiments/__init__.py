from src.utils.to_dict import yaml_to_dict
## Paths
d_conf = yaml_to_dict('config/global.yml')
SKETCHGRAPHS_PATH = d_conf['path']['sketchgraphs_path']
COARSE_PATH = d_conf['path']['coarse_path']
NORMALIZATION_PATH = d_conf['path']['normalization_path']
INDEXES_PATH = d_conf['path']['indexes_path']
WEIGHTS_PATH = d_conf['path']['weights_path']
ENCODING_PATH = d_conf['path']['encoding_path']
PARAMETERS_PATH = d_conf['path']['parameters_path']

## Global params 
# (these params can be changed but should stay consistent through the pipeline steps)
L_MAX = d_conf['parameters']['lMax']
L_MIN = d_conf['parameters']['lMin']
DOF_MAX = d_conf['parameters']['dof_max']
N_BINS = d_conf['parameters']['n_bins']

L_KEEP_EDGE = d_conf['keep']['l_keep_edge']
L_KEEP_NODE = d_conf['keep']['l_keep_node']
