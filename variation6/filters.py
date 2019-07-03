import dask.array as da
import numpy as np

from variation6 import (GT_FIELD, DP_FIELD, MISSING_INT, QUAL_FIELD,
                        PUBLIC_CALL_GROUP)
from variation6.variations import Variations
from variation6.result import Result
from variation6.stats import calc_missing_gt

FLT_VARS = 'flt_vars'


def filter_by_min_call(variations, min_calls, rates=True):
    num_missing_gts = calc_missing_gt(variations, rates=rates)['num_missing_gts']
    selected_vars = num_missing_gts >= min_calls
    variations = variations.get_vars(selected_vars)
    return Result({FLT_VARS: variations})


def _gt_to_missing(variations, field, min_value):
    gts = variations[GT_FIELD]
    calls_setted_to_missing = variations[field] < min_value

    # as we can not slice using arrays of diferente dimensions, we need to
    # create one with same dimensions with stack
    p2 = da.stack([calls_setted_to_missing, calls_setted_to_missing], axis=2)
    gts[p2] = MISSING_INT

    variations[GT_FIELD] = gts

    return Result({FLT_VARS: variations})


def min_depth_gt_to_missing(variations, min_depth):
    return _gt_to_missing(variations, field=DP_FIELD, min_value=min_depth)


def min_qual_gt_to_missing(variations, min_qual):
    return _gt_to_missing(variations, field=QUAL_FIELD, min_value=min_qual)


def filter_samples(variations, samples):

    samples_in_variation = variations.samples.compute()
    sample_cols = np.array(sorted(list(samples_in_variation).index(sample) for sample in samples))

    new_variations = Variations(samples=da.from_array(samples))
    for field, array in variations._arrays.items():
        if PUBLIC_CALL_GROUP in field:
            array = array[:, sample_cols]
        new_variations[field] = array
    return Result({FLT_VARS: new_variations})
