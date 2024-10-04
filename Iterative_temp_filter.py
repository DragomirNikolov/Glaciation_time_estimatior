import numpy as np
import netCDF4 as nc
import matplotlib.pyplot as plt
import time
from Helper_fun import time_intersection,generate_temp_range
# ==================================================
# Create a merged file that includes the cph field
# ==================================================


def output_file_generation(comb_mask,y_limited_ind_bug, dates, min_temp, max_temp):
    date = dates[0]
    new_file_name = f"/wolke_scratch/dnikolo/TEST/example_preprocessing/CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-{date.day:02d}-{date.month:02d}-{date.year}_{date.hour:02d}:{date.minute:02d}:{date.second:02d}.nc"

    # Create a new NetCDF file
    new_dataset = nc.Dataset(new_file_name, 'w', format='NETCDF4')

    new_dataset.set_fill_off()
    new_dataset.createDimension("lon", len(x))
    new_dataset.createDimension("lat", len(y_limited_ind))
    new_dataset.createDimension('time', len(time_ind_cph))
    new_dataset.createDimension('object_num', 2)
    # Copy variables from the original file, except the time variable
    for var_name, var in cph_data.variables.items():
        if var_name == 'time':
            new_time = new_dataset.createVariable('time', var.dtype, ('time',))
            new_time.units = time_units
            new_time.calendar = calendar
            new_time[:] = nc.date2num(
                dates[time_ind_cph], units=time_units, calendar=calendar)

        elif var_name=="x":
            new_var = new_dataset.createVariable('lon', var.dtype, ('lon',))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var.long_name="longitude"
            new_var[:] =  x*10*180/np.pi  # For time-dependent variables, use the ith timestep
        elif var_name=="y":
            new_var = new_dataset.createVariable('lat', var.dtype, ('lat',))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var.long_name="latitude"
            new_var[:] = y[y_limited_ind_bug]*10*180/np.pi  # For time-dependent variables, use the ith timestep
        elif var_name=="cph":
            new_var = new_dataset.createVariable(var_name, var.dtype, ('time','lat','lon'))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var[:] = ~(comb_mask) # For time-dependent variables, use the ith timestep

        elif var_name == "cph":
            new_var = new_dataset.createVariable(
                var_name, var.dtype, var.dimensions)
            # Copy variable attributes
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})
            new_var.coordinates = "x y"
            # For time-dependent variables, use the ith timestep -(cloud_mask.mask[count,:,:]-1)*50
            new_var[:] = ~(comb_mask)

        else:
            new_var = new_dataset.createVariable(
                var_name, var.dtype, var.dimensions)
            # Copy variable attributes
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})
            # could also be : ... =var[i] if 'time' in var.dimensions else  var[:]
            new_var[:] = var[:]
            # For time-dependent variables, use the ith timestep

    # Close the new file
    new_dataset.close()

# ==================================================
# Filtering and formatting data
# ==================================================


def filter_data(ctt, cph, min_temp, max_temp):
    # Filter by temperature range -38C<T<0C
    ctt = np.ma.masked_where((ctt < 273.15 + min_temp)
                             | (ctt > 273.15 + max_temp), ctt)
    # Create a combined mask that only has entries at positions within temp range and valid phase
    comb_mask = ctt.mask | cph.mask
    return comb_mask


# Set up timer
start_time = time.time()

# ==================================================
# Open and load cloud top temp and cloud top phase data
# ==================================================

print("Loading data")
cph_fp = '/wolke_scratch/dnikolo/TEST/cph.CPP.nc.nc'
tmp_fp = '/wolke_scratch/dnikolo/TEST/ctt.nc.nc'
cph_data = nc.Dataset(cph_fp)  # cloud_phase_file
tmp_data = nc.Dataset(tmp_fp)  # cloud_phase_file
print(f"Data loaded. Elapsed time: {time.time()-start_time}")


# ==================================================
# ==================================================


# ==================================================
# Load axices
# ==================================================

print(f"Setting up dimentions")

# Space dimentions
# ==================================================
x = tmp_data['x'][:]
y = tmp_data['y'][:]

# Limit data to a region and time period
# Limit between -35 and -75 degree lat
y_limited_ind_bug = np.where((y < -0.061) & (y > -0.1396))[0]
y_limited_ind = tuple(y_limited_ind_bug)

# Generate lat and lon matrices
lon_mat = np.ones((len(y_limited_ind), len(x)))*(x.T * 10 * 180 / np.pi)
lat_mat = np.ones((len(y_limited_ind), len(x))) * \
    (y[y_limited_ind_bug]*10*180/np.pi)[:, None]

# Time dimension
# ==================================================

# Get time intersection
time_ind_ctt, time_ind_cph = time_intersection(
    tmp_data['time'], cph_data['time'])

# Extract time variable for later use
# assuming the time variable is named 'time'
time_var = cph_data.variables['time']
time_units = time_var.units
calendar = time_var.calendar if hasattr(time_var, 'calendar') else 'standard'

# Convert time steps to dates using netCDF num2date
dates = nc.num2date(time_var[:], units=time_units, calendar="gregorian")

print(f"Dimentions set. Elapsed time: {time.time()-start_time}")

# ==================================================
# ==================================================

# ==================================================
# Filter data and generate new files
# ==================================================

print("Loading copies of data")
# Get reduced data arrays ctt and cph
ctt = tmp_data['ctt'][time_ind_ctt, y_limited_ind, :]
cph = cph_data['cph'][time_ind_cph, y_limited_ind, :]

t_deltas=[12,19]
temp_bounds=generate_temp_range(t_deltas)

for min_temp,max_temp in temp_bounds:
    filter_mask = filter_data(ctt, cph, min_temp, max_temp)
    print(f"Generating new merged file: T={min_temp}:{max_temp}")
    output_file_generation(
        filter_mask, y_limited_ind_bug, dates, min_temp, max_temp)
    print(
        f"New merged file generated and saved. Elapsed time {time.time()-start_time}")


# ==================================================
# ==================================================
