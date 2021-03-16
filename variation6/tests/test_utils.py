
import sys
import os

sys.path.insert(0, os.path.abspath('../..'))

from variation6.in_out.zarr import load_zarr
from variation6.tests import TEST_DATA_DIR
from variation6.filters import remove_low_call_rate_vars
from variation6 import FLT_VARS, DEFAULT_VARIATION_NUM_IN_CHUNK


def create_dask_variations(num_vars_per_chunk=DEFAULT_VARIATION_NUM_IN_CHUNK):
    return load_zarr(TEST_DATA_DIR / 'test.zarr',
                     num_vars_per_chunk=num_vars_per_chunk)


def create_non_materialized_snp_filtered_variations():
    variations = create_dask_variations()
    return remove_low_call_rate_vars(variations, min_call_rate=0)[FLT_VARS]
