import numpy as np
import netCDF4 as nc

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

