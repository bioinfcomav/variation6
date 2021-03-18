import math
import re

import dask.array as da
import numpy as np

from variation6.compute import compute
from variation6 import MISSING_VALUES

DEF_NUM_BINS = 40


def _same_interface_funcs(funcname, array, *args, **kwargs):
    if isinstance(array, da.Array):
        module = da
    elif isinstance(array, np.ndarray):
        module = np
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)
    func = getattr(module, funcname)
    return func(array, *args, **kwargs)


def sum(array, *args, **kwargs):  # @ReservedAssignment
    return _same_interface_funcs('sum', array, *args, **kwargs)


def min(array, *args, **kwargs):  # @ReservedAssignment
    return _same_interface_funcs('min', array, *args, **kwargs)


def max(array, *args, **kwargs):  # @ReservedAssignment
    return _same_interface_funcs('max', array, *args, **kwargs)


def isnan(array, *args, **kwargs):
    return _same_interface_funcs('isnan', array, *args, **kwargs)


def isinf(array, *args, **kwargs):
    return _same_interface_funcs('isinf', array, *args, **kwargs)


def histogram(vector, n_bins, limits, weights=None):

    if n_bins is None:
        n_bins = DEF_NUM_BINS

    try:
        dtype = vector.dtype
    except AttributeError:
        dtype = type(vector[0])

    missing_value = MISSING_VALUES[dtype]

    if weights is None:
        if math.isnan(missing_value):
            not_nan = ~isnan(vector)
        else:
            not_nan = vector != missing_value

        vector = vector[not_nan]

    if isinstance(vector, da.Array):
        histo = da.histogram
        if limits is None:
            raise ValueError('Limits is mandatory to use this function')

    else:
        histo = np.histogram
        if limits is None:
            limits = (min(vector), max(vector))

    try:
        dtype = vector.dtype
    except AttributeError:
        dtype = type(vector[0])

    missing_value = MISSING_VALUES[dtype]

    if weights is None:
        if math.isnan(missing_value):
            not_nan = ~isnan(vector)
        else:
            not_nan = vector != missing_value

        vector = vector[not_nan]

    try:
        result = histo(vector, bins=n_bins, range=limits, weights=weights)
    except ValueError as error:
        if ('parameter must be finite' in str(error) or
                re.search('autodetected range of .*finite', str(error))):
            isfinite = ~isinf(vector)
            vector = vector[isfinite]
            if weights is not None:
                weights = weights[isfinite]

            result = histo(vector, bins=n_bins, range=limits, weights=weights)
        else:
            raise
    return result


def count_nonzero(a, *args, **kwargs):
    return _same_interface_funcs('count_nonzero', a, *args, **kwargs)


def stack(a, *args, **kwargs):
    array_used_to_infere_type = kwargs.pop('as_type_of', None)
    if array_used_to_infere_type is None:
        array_used_to_infere_type = a
    if isinstance(array_used_to_infere_type, da.Array):
        module = da
    elif isinstance(array_used_to_infere_type, (np.ndarray, list, tuple)):
        module = np
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)

    return module.stack(a, *args, **kwargs)


def amax(array, *args, **kwargs):
    if isinstance(array, da.Array):
        function = da.max
    elif isinstance(array, np.ndarray):
        function = np.amax
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)
    return function(array, *args, **kwargs)


def create_full_array_in_memory(shape, fill_value, *args, **kwargs):
    return np.full(shape, fill_value, *args, **kwargs)


def logical_and(cond1, cond2, *args, **kwargs):
    if isinstance(cond1, da.Array) or isinstance(cond2, da.Array):
        function = da.logical_and
    elif isinstance(cond1, np.ndarray) or isinstance(cond2, np.ndarray):
        function = np.logical_and
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)
    return function(cond1, cond2, *args, **kwargs)


def logical_not(cond1, *args, **kwargs):
    if isinstance(cond1, da.Array):
        function = da.logical_not
    elif isinstance(cond1, np.ndarray):
        function = np.logical_not
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)
    return function(cond1, *args, **kwargs)


def logical_or(cond1, cond2, *args, **kwargs):
    if isinstance(cond1, da.Array) or isinstance(cond2, da.Array):
        function = da.logical_or
    elif isinstance(cond1, np.ndarray) or isinstance(cond2, np.ndarray):
        function = np.logical_or
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)
    return function(cond1, cond2, *args, **kwargs)


def any(array, *args, **kwargs):  # @ReservedAssignment
    return _same_interface_funcs('any', array, *args, **kwargs)


def all(array, *args, **kwargs):  # @ReservedAssignment
    return _same_interface_funcs('all', array, *args, **kwargs)


def add(array1, array2, *args, **kwargs):
    if isinstance(array1, da.Array) or isinstance(array2, da.Array):
        function = da.add
    elif isinstance(array1, np.ndarray) or isinstance(array2, np.ndarray):
        function = np.add
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)

    return function(array1, array2, *args, **kwargs)


def nanmean(array, *args, **kwargs):
    return _same_interface_funcs('nanmean', array, *args, **kwargs)


def isfinite(array, *args, **kwargs):
    return _same_interface_funcs('isfinite', array, *args, **kwargs)


def nansum(array, *args, **kwargs):
    return _same_interface_funcs('nansum', array, *args, **kwargs)


def create_not_initialized_array_in_memory(*args, **kwargs):
    return np.empty(*args, kwargs)


def full(shape, *args, **kwargs):
    try:
        array_used_to_infere_type = kwargs.pop('as_type_of')
    except KeyError:
        msg = 'as_type_of is mandatory:  This is an array to infer the type '
        msg += 'of the generated array'
        raise ValueError(msg)

    if isinstance(array_used_to_infere_type, da.Array):
        return da.full(shape, *args, **kwargs)
    elif isinstance(array_used_to_infere_type, np.ndarray):
        return np.full(shape, *args, **kwargs)
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)


def ones(shape, *args, **kwargs):
    try:
        array_used_to_infere_type = kwargs.pop('as_type_of')
    except KeyError:
        msg = 'as_type_of is mandatory:  This is an array to infer the type '
        msg += 'of the generated array'
        raise ValueError(msg)

    if isinstance(array_used_to_infere_type, da.Array):
        return da.ones(shape, *args, **kwargs)
    elif isinstance(array_used_to_infere_type, np.ndarray):
        return np.ones(shape, *args, **kwargs)
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)


def empty_array(same_type_of):
    if isinstance(same_type_of, da.Array):
        return da.from_array(np.array([]))
    elif isinstance(same_type_of, np.ndarray):
        return np.array([])
    else:
        msg = 'Not implemeted for type not in dask array or numpy ndarray'
        raise NotImplementedError(msg)


def map_blocks(func, *args, **kwargs):
    array = args[0]
    if isinstance(array, da.Array):
        return da.map_blocks(func, *args, **kwargs)
    else:
        return func(*args)


def make_sure_array_is_in_memory(array, silence_runtime_warnings=False):
    if isinstance(array, da.Array):
        array = compute(array, silence_runtime_warnings=silence_runtime_warnings)
    return array


def pack(*args):
    if isinstance(args[0], da.Array):
        v = da.from_array(args)
        return v
    else:
        return list(args)


###############################################################################
# functions used just once. these are very specific, I gues wrongly implemented
###############################################################################
def assign_with_mask(array, using, mask):
    if isinstance(array, da.Array):
        values_to_modify = using
    else:
        values_to_modify = using[mask]

    array[mask] = values_to_modify


def reshape_if_needed(array, mask):
    if (array.shape and isinstance(array, da.Array) and
        not np.any(np.isnan(array.shape)) and
        len(array.shape) != len(mask.shape)):

        mask = mask.reshape((array.shape))
    return mask


def samples_to_numpy_str(samples):
    samples = [sample.decode() for sample in make_sure_array_is_in_memory(samples)]
    return samples


# rara
def reduce_chunk_dimensions(array):
    chunks = None
    if isinstance(array, da.Array):
        chunks = (array.chunks[0], (1,) * len(array.chunks[1]))
    return chunks
###############################################################################
