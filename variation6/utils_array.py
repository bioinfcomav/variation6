
import math

from variation6 import NotMaterializedError


def get_shape_item(array, idx: int, check_materilized=True):
    item = array.shape[idx]
    if check_materilized and math.isnan(item):
        raise NotMaterializedError()
    return item
