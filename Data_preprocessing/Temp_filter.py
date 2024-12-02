import numpy as np
import netCDF4 as nc
from ..Helper_fun import generate_temp_range
# ==================================================
# Filtering and formatting data
# ==================================================

# TODO: Make non-cph specific


def filter_data(ctt, cph, min_temp, max_temp):
    # Filter by temperature range -38C<T<0C
    ctt = np.ma.masked_where((ctt < 273.15 + min_temp)
                             | (ctt > 273.15 + max_temp), ctt)
    # Create a combined mask that only has entries at positions within temp range and valid phase
    cph_filtered = cph.copy()
    cph_filtered.mask = ctt.mask | cph.mask
    return cph_filtered


def iterative_filter():
    # TODO: Add dt to config file
    t_deltas = [5]  # [2, 5 , 10 , 15 , 38]
    temp_bounds = generate_temp_range(t_deltas)
    generate_temp_range(dt)
