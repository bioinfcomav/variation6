
from pathlib import Path

from variation6 import ZARR, H5PY


def _get_file_type_from_magic_number(path):
    file_start = path.open('rb').read(32)

    hex_bytes = ['{:02X}'.format(byte) for byte in file_start]
    if hex_bytes[:8] == ['89', '48', '44', '46', '0D', '0A', '1A', '0A']:
        return H5PY

    raise NotImplementedError('Unknown file type')


def _get_file_from_suffix(path):
    suffix = path.suffix
    if suffix in ('.hdf5', '.h5'):
        return H5PY
    elif suffix == '.zarr':
        return ZARR
    raise NotImplementedError('Unknown file type')


def get_var_file_type(path):
    if not path.exists():
        return _get_file_from_suffix(path)

    if path.is_dir():
        if path.suffix == '.zarr':
            return ZARR
        else:
            raise NotImplementedError('Is a directory, but not zarr')
    else:
        return _get_file_type_from_magic_number(path)


if __name__ == '__main__':
    print(get_var_file_type(Path('/home/jope/devel3/variation6/variation6/tests/data/test.zarr')))