import numpy as np
import netCDF4 as nc
from ..Helper_fun import generate_temp_range
# ==================================================
# Filtering and formatting data
# ==================================================

# TODO: Make non-cph specific


class TempFilteredOutputGen:
    def __init__(self, ctt, cph):
        self.ctt = ctt
        self.cph = cph

    def filter_data(self, min_temp, max_temp):
        # Filter by temperature range -38C<T<0C
        ctt_filtered = np.ma.masked_where((self.ctt < 273.15 + min_temp)
                                 | (self.ctt > 273.15 + max_temp), ctt)
        # Create a combined mask that only has entries at positions within temp range and valid phase
        cph_filtered = self.cph.copy()
        cph_filtered.mask = ctt_filtered.mask | self.cph.mask
        return cph_filtered

    def iterative_filter(self, t_deltas, filename):
        temp_bounds = generate_temp_range(t_deltas)
        for i in range(temp_bounds[0].shape[0]):
            min_temp = temp_bounds[0][i]
            max_temp = temp_bounds[1][i]
            cph_filtered = self.filter_data(min_temp, max_temp)
            output=OutputFile()
            


