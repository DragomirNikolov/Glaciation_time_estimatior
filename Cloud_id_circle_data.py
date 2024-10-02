import numpy as np
import netCDF4 as nc
import matplotlib.pyplot as plt


#####
#find time indexes at which there is data from both datasets
#returns (indexes of common time in time1 , ---//--- in time2)
#####
def time_intersection(time1,time2):
    return (np.nonzero(np.in1d(time1, time2))[0] , np.nonzero(np.in1d(time2, time1))[0])

#Open and load cloud top temp and cloud top phase data
cph_fp='/wolke_scratch/dnikolo/TEST/cph.CPP.nc.nc'
tmp_fp='/wolke_scratch/dnikolo/TEST/ctt.nc.nc'
cph_data = nc.Dataset(cph_fp)#cloud_phase_file
tmp_data = nc.Dataset(tmp_fp)#cloud_phase_file

#Load axices
x=tmp_data['x'][:]
y=tmp_data['y'][:]

#Limit data to a region and time period
#Limit between -35 and -75 degree lat
y_limited_ind_bug=np.where((y<-0.061) & (y>-0.1396))[0]
y_limited_ind=tuple(y_limited_ind_bug)

#get time intersection
time_ind_ctt, time_ind_cph = time_intersection(tmp_data['time'], cph_data['time'])

#####
#Generate circle dataset of a circle moving left
#####
centery, radius = int(len(y_limited_ind)/2), 90
radius2=radius*1.2
shape=(len(time_ind_cph),len(y_limited_ind),len(x))

# for i in range(len(time_ind_cph)):
cloud_mask = np.ma.array(np.ones(shape), mask=np.zeros(shape))
x_mesh = np.arange(cloud_mask.shape[1])
y_mesh = np.arange(cloud_mask.shape[2])
t_mesh = np.arange(cloud_mask.shape[0])
X, T, Y = np.meshgrid(x_mesh, t_mesh, y_mesh)
feature_varname=cloud_mask
cloud_mask=np.ma.masked_where(~(((Y - shape[2]/2- 10*T)**2 + (X - centery -int(radius*1.5))**2 > radius**2)^((Y - shape[2]/2- 10*T)**2 + (X - centery+int(radius*1.5))**2 > radius2**2)),cloud_mask)
feature_varname=np.zeros(shape)
feature_varname[np.where(~((Y - shape[2]/2- 10*T)**2 + (X - centery -int(radius*1.5))**2 > radius**2))]=1
feature_varname[np.where(~((Y - shape[2]/2- 10*T)**2 + (X - centery + int(radius*1.5)  )**2 > radius2**2))]=2
# Extract the time variable
# ==================================================
time_var = cph_data.variables['time']  # assuming the time variable is named 'time'
time_units = time_var.units
calendar = time_var.calendar if hasattr(time_var, 'calendar') else 'standard'
print("Calculating lon_mat and lat_mat")
lon_mat= np.ones((len(y_limited_ind), len(x)))*(x.T * 10 * 180 / np.pi)
lat_mat=np.ones((len(y_limited_ind), len(x)))*(y[y_limited_ind_bug]*10*180/np.pi)[:,None]
# Convert time steps to dates using netCDF num2date
dates = nc.num2date(time_var[:], units=time_units, calendar="gregorian")


##Generate cloud id files
count=0
for i in time_ind_cph:
    date=dates[i]
    new_file_name = f"/wolke_scratch/dnikolo/TEST/example_postprocessing/tracking/cloudid_{date.year}{date.month:02d}{date.day:02d}_{date.hour:02d}{date.minute:02d}{date.second:02d}.nc"
    # Create a new NetCDF file
    new_dataset = nc.Dataset(new_file_name, 'w', format='NETCDF4')

    new_dataset.set_fill_off()
    new_dataset.createDimension("lon", len(x))
    new_dataset.createDimension("lat", len(y_limited_ind))
    new_dataset.createDimension('time', 1)
    new_dataset.createDimension('object_num',2)
    # Copy variables from the original file, except the time variable
    print("a")
    for var_name, var in cph_data.variables.items():
        if var_name == 'time':
            new_time = new_dataset.createVariable('time', var.dtype, ('time',))
            new_time.units = time_units
            new_time.calendar = calendar
            new_time[:] = nc.date2num([date], units=time_units, calendar=calendar)
        elif var_name=="x":
            new_var = new_dataset.createVariable('longitude', var.dtype, ('lat','lon'))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var.long_name="longitude"
            new_var[:] =  lon_mat  # For time-dependent variables, use the ith timestep
        elif var_name=="y":
            new_var = new_dataset.createVariable('latitude', var.dtype, ('lat','lon'))
            new_var.long_name="latitude"
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var[:] = lat_mat  # For time-dependent variables, use the ith timestep
        elif var_name=="cph":
            new_var = new_dataset.createVariable(var_name, var.dtype, ('time','lat','lon'))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var[:] = ~(cloud_mask.mask[count,:,:]) # For time-dependent variables, use the ith timestep -(cloud_mask.mask[count,:,:]-1)*50

            new_var = new_dataset.createVariable("feature_number", var.dtype, ('time','lat','lon'))
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var[:] = feature_varname[count,:,:] # For time-dependent variables, use the ith timestep -(cloud_mask.mask[count,:,:]-1)*50

        else:
            new_var = new_dataset.createVariable(var_name, var.dtype, var.dimensions)
            new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
            new_var[:] =  var[:]  #could also be : ... =var[i] if 'time' in var.dimensions else  var[:]
                                    # For time-dependent variables, use the ith timestep
    new_var=new_dataset.createVariable('nfeatures', 'i2')
    new_var.assignValue(2)
    
    basetime=new_dataset.createVariable('base_time','f8',('time',),fill_value=np.nan)
    basetime.units="Seconds since 1970-1-1 0:00:00 0:00"
    basetime.long_name = "Base time in Epoch"
    basetime[:]=nc.date2num([date], units=basetime.units)

    npix_feature=new_dataset.createVariable('npix_feature', 'u8', ('object_num'))
    npix_feature[0]=np.count_nonzero( feature_varname[1,:,:] == 1)
    npix_feature[1]=np.count_nonzero( feature_varname[1,:,:] == 2)

    # Close the new file
    new_dataset.close()
    count+=1



#Create a merged file that includes the cph field
count=0

date=dates[0]
new_file_name = f"/wolke_scratch/dnikolo/TEST/example_preprocessing/merged-{date.day:02d}-{date.month:02d}-{date.year}_{date.hour:02d}:{date.minute:02d}:{date.second:02d}.nc"

# Create a new NetCDF file
new_dataset = nc.Dataset(new_file_name, 'w', format='NETCDF4')

new_dataset.set_fill_off()
new_dataset.createDimension("x", len(x))
new_dataset.createDimension("y", len(y_limited_ind))
new_dataset.createDimension('time', len(time_ind_cph))
new_dataset.createDimension('object_num',2)
# Copy variables from the original file, except the time variable
print("b")
for var_name, var in cph_data.variables.items():
    if var_name == 'time':
        new_time = new_dataset.createVariable('time', var.dtype, ('time',))
        new_time.units = time_units
        new_time.calendar = calendar
        new_time[:] = nc.date2num(dates[time_ind_cph], units=time_units, calendar=calendar)
    elif var_name=="x":
        new_var = new_dataset.createVariable(var_name, var.dtype, ('x'))
        new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
        new_var[:] = x*10*180/np.pi  # For time-dependent variables, use the ith timestep
    elif var_name=="y":
        new_var = new_dataset.createVariable(var_name, var.dtype, ('y'))
        new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
        new_var[:] = y[y_limited_ind_bug]*10*180/np.pi  # For time-dependent variables, use the ith timestep
    
    elif var_name=="cph":
        new_var = new_dataset.createVariable(var_name, var.dtype, var.dimensions)
        new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
        new_var[:] = ~(cloud_mask.mask[:,:,:]) # For time-dependent variables, use the ith timestep -(cloud_mask.mask[count,:,:]-1)*50

        # new_var = new_dataset.createVariable("feature_varname", var.dtype, var.dimensions)
        # new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
        # new_var[:] = feature_varname[:,:,:] # For time-dependent variables, use the ith timestep -(cloud_mask.mask[count,:,:]-1)*50

    else:
        new_var = new_dataset.createVariable(var_name, var.dtype, var.dimensions)
        new_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})  # Copy variable attributes
        new_var[:] =  var[:]  #could also be : ... =var[i] if 'time' in var.dimensions else  var[:]
                                # For time-dependent variables, use the ith timestep
# new_var=new_dataset.createVariable('nfeatures', 'i2')
# new_var.assignValue(2)

# npix_feature=new_dataset.createVariable('npix_feature', 'u8', ('object_num'))
# npix_feature[0]=np.count_nonzero( feature_varname[1,:,:] == 1)
# npix_feature[1]=np.count_nonzero( feature_varname[1,:,:] == 2)

# Close the new file
new_dataset.close()