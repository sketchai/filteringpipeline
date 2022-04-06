import glob

from src.utils import flat_array
import numpy as np
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

def merge_array_slices(slice_path_list: list[str]) -> np.ndarray:
    """
    This script merges the slices into a single flat array.
    As it requires too much memory to unpickle all slices into python objects
    we only manipulate the offsets and raw data bytes
    
    Args:
        slice_list_path
    Returns:
        merged_array: raw np.array that can be read by the flat_array module
    """
    list_of_offsets = []
    list_of_databytes = []
    for file_path in slice_path_list:
        try:
            slice_array = flat_array.load_dictionary_flat(file_path)['sequences']
        except BaseException:
            slice_array = flat_array.load_flat_array(file_path)
        list_of_offsets.append(slice_array._offsets)
        list_of_databytes.append(slice_array._pickle_data)        
    
    all_offsets, all_databytes = merge_raw_list(list_of_offsets, list_of_databytes)
    merged_array = flat_array.pack_list_flat(all_offsets, all_databytes) # add metadata

    return merged_array

def merge_raw_list(offset_arrays, data_arrays):
    """Merges a list of raw flat lists (as produced by `raw_list_flat`) into one single raw flat list.

    Parameters
    ----------
    offset_arrays: list of np.ndarray
        A list of arrays representing the offsets.
    data_arrays: list of np.ndarray
        A list of the same length as ``offset_arrays`` representing the data.

    Returns
    -------
    np.ndarray
        An array of offsets, of length one plus the number of elements in the data.
    np.ndarray
        An array of bytes, representing the concatenate serialized data.

    ----------------------------
    Source : SketchGraphs data. sketchgraphs/data/flat_array.py
    (modified to fix an offset problem)
    """
    i64 = np.dtype('<i8')
    total_sketches = sum(len(off) - 1 for off in offset_arrays)
    all_offsets = np.empty(total_sketches + 1, dtype=i64)
    current_offset = 0
    idx = 0
    len_data_arrays = [len(data) for data in data_arrays]

    for off, len_data in zip(offset_arrays, len_data_arrays):
        all_offsets[idx:idx + len(off) - 1] = off[:-1] + current_offset
        # current_offset += off[-1]
        current_offset += len_data
        idx += len(off) - 1

    all_offsets[-1] = current_offset
    all_data = np.concatenate(data_arrays)

    return all_offsets, all_data