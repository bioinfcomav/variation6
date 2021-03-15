import test_config
import unittest
import numpy as np

from variation6.variations import Variations
from variation6 import GT_FIELD, CHROM_FIELD, NotMaterializedError
from variation6.tests import TEST_DATA_DIR
from variation6.in_out.zarr import load_zarr
from variation6.filters import remove_low_call_rate_vars, FLT_VARS


def _load_one_dask():
    variations = load_zarr(TEST_DATA_DIR / 'test.zarr')
    return variations


def _create_empty_dask_variations():
    variations = _load_one_dask()
    return remove_low_call_rate_vars(variations, min_call_rate=1.1)[FLT_VARS]


class VariationsTest(unittest.TestCase):

    def test_basic_operations(self):
        variations = Variations()
        self.assertEqual(variations.num_variations, 0)
        self.assertEqual(variations.num_samples, 0)

        gts = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3]])
        # trying to add call data without samples fails
        with self.assertRaises(ValueError) as _:
            variations[GT_FIELD] = gts

        # set samples
        variations.samples = ['1', '2', '3']
        self.assertEqual(variations.num_samples, 3)

        # adding again samples fails
        with self.assertRaises(RuntimeError) as _:
            variations.samples = ['1', '2', '3']

        # add variationData
        chroms = np.array(['chr1', 'chr2', 'chr3'])
        variations[CHROM_FIELD] = chroms

        # add data with wrong shape
        with self.assertRaises(ValueError) as context:
            variations[GT_FIELD] = gts = np.array([[1, 2, 3]])
        self.assertIn('Introduced matrix shape', str(context.exception))

        with self.assertRaises(ValueError) as context:
            variations[GT_FIELD] = gts = np.array([[1, 2], [1, 2], [1, 2]])
        self.assertIn('not fit with num samples',
                       str(context.exception))

        # set gt array
        gts = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3]])
        variations[GT_FIELD] = gts
        self.assertTrue(np.array_equal(gts, variations[GT_FIELD]))
        self.assertEqual(variations.num_variations, 3)

    def test_iterate_chunks(self):
        # in memory
        variations = Variations()
        variations.samples = ['1', '2', '3']
        gts = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3]])
        variations[GT_FIELD] = gts
        for index, chunk in enumerate(variations.iterate_chunks(chunk_size=1)):
            assert np.all(chunk[GT_FIELD] == variations[GT_FIELD][index, :])
            assert np.all(chunk.samples == variations.samples)

        # in disk
        variations = load_zarr((TEST_DATA_DIR / 'test.zarr'), num_vars_per_chunk=1)
        chunks = list(variations.iterate_chunks())
        self.assertEqual(len(chunks), 7)

    def test_unavailable_shape(self):
        variations = Variations()
        variations.samples = ['1', '2', '3']
        gts = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3]])
        variations[GT_FIELD] = gts
        assert variations.num_variations == 3

        variations = _create_empty_dask_variations()
        try:
            variations.num_variations
            self.fail('NotMaterializedError expected')
        except NotMaterializedError:
            pass

if __name__ == "__main__":
    unittest.main()
