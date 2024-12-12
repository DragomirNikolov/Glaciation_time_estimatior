import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import time
from Single_cloud_analysis import Cloud
from Helper_fun import generate_temp_range
import os
import datetime as dt
import math
import numba as nb


@nb.njit
def extract_cloud_coordinates(cloudtracknumber_field, cloud_id_in_field, max_size):
    # Define the dictionary with the appropriate types
    loc_hash_map_cloud_numbers = {
        j: (0, np.zeros((2, max_size), dtype=np.int16)) for j in cloud_id_in_field}
    # # Traverse the 3D array
    # for i in cloud_id_in_field:
    #     loc_hash_map_cloud_numbers[val] = (0,np.empty((2,max_size),dtype=np.int16))
    for row in range(cloudtracknumber_field.shape[1]):
        for col in range(cloudtracknumber_field.shape[2]):
            val = cloudtracknumber_field[0, row, col]
            if val != 0:
                ind, cord = loc_hash_map_cloud_numbers[val]
                if ind <= max_size:
                    cord[:, ind] = np.asarray([row, col], dtype=np.int16)
                    ind += 1
                    # print(ind)
                    loc_hash_map_cloud_numbers[val] = (ind, cord)
    return loc_hash_map_cloud_numbers
    # return loc_hash_map_cloud_numbers


def analize_single_temp_range(temp_ind, fast_mode, write_csv, agg_fact):
    # loop_start_time=dt.datetime.now()
    min_temp, max_temp = min_temp_array[temp_ind], max_temp_array[temp_ind]
    # Load datasets
            current_iteration_dir = job_output_dir + \
                f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}/'
        cloudtrack_data = nc.Dataset(
            current_iteration_dir+'/pixel_path_tracking/20040201.1415_20040201.2000/cloudtracks_20040201_183000.nc')
        trackstats_data = nc.Dataset(
            current_iteration_dir+'/stats/trackstats_final_20040201.1415_20040201.2000.nc')
        tracknumbers_data = nc.Dataset(
            current_iteration_dir+'/stats/tracknumbers_20040201.1415_20040201.2000.nc')
    except:  # Exception as inst:
        print(f"Skipping {min_temp} to {max_temp}")
        # print(type(inst))    # the exception type

        # print(inst.args)     # arguments stored in .args

        # print(inst)          # __str__ allows args to be printed directly,

        # but may be overridden in exception subclasses
        cloud_list.append([])
        continue
    # Load relevant data from datasets into local variables
    cloudtracknumber_field = cloudtrack_data.variables['tracknumber'][:, :, :]
    cph_field = cloudtrack_data.variables['cph'][:, :, :]
    n_tracks = trackstats_data.variables['track_duration'].shape[0]
    basetimes = tracknumbers_data.variables['basetimes'][:]
    lat = cloudtrack_data.variables['lat'][:]
    lon = cloudtrack_data.variables['lon'][:]
    lat_resolution = (lat.max()-lat.min())/len(lat)
    lon_resolution = (lon.max()-lon.min())/len(lon)
    trackstats_data.close()
    tracknumbers_data.close()
    cloudtrack_data.close()

    sum_load_track_variables += append_start_time-loop_start_time
    # print(append_start_time-loop_start_time)
    cloud_list.append([Cloud(f'{temp_ind}_{i}') for i in range(n_tracks)])
    sum_append_cloud += append_end_time-append_start_time
    # print(append_end_time-append_start_time)
    print(f'Analyzing T: {min_temp} to {max_temp} Agg={agg_fact}')
    for unix_time in basetimes:
        time = dt.datetime.utcfromtimestamp(unix_time)
        time_str = time.strftime("%Y%m%d_%H%M%S")
        print(f'{min_temp} to {max_temp} Loading {time_str}')
        cloudtrack_fp = current_iteration_dir + \
            f'/pixel_path_tracking/20040201.1415_20040201.2000/cloudtracks_{time_str}.nc'
        cloudtrack_data = nc.Dataset(cloudtrack_fp)
        cloudtracknumber_field = cloudtrack_data.variables['tracknumber'][:, :, :].data
        cph_field = cloudtrack_data.variables['cph'][:, :, :]
        cloudtrack_data.close()
        sum_data_loading += analysis_start_time-data_loading_start_time
        cloud_id_in_field, counts = np.unique(
            cloudtracknumber_field, return_counts=True)
        counts = counts[cloud_id_in_field != 0]
        cloud_id_in_field = cloud_id_in_field[cloud_id_in_field != 0]
        max_allowed_cloud_size_px = 20000 if fast_mode else counts.max()
        hash_map_cloud_numbers = extract_cloud_coordinates(
            cloudtracknumber_field, cloud_id_in_field, max_allowed_cloud_size_px)  # counts.max())
        if max_allowed_cloud_size_px > 1000000:
            print(np.where(counts, counts == counts.max()))
        for track_number in cloud_id_in_field:
            try:
                current_cloud = cloud_list[temp_ind][track_number-1]
            except:
                print(
                    f"Error: {temp_ind,track_number,len(cloud_list[temp_ind])}")
                continue
            if (not current_cloud.terminate_cloud):
                # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
                ind, cord = hash_map_cloud_numbers[track_number]
                cloud_location_ind = [cord[0, :ind], cord[1, :ind]]
                if cloud_location_ind[0].size != 0:
                    avg_lat_ind = int(round(np.mean(cloud_location_ind[0])))
                    avg_lon_ind = int(round(np.mean(cloud_location_ind[1])))
                    # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
                    cloud_values = cph_field[0,
                                             cloud_location_ind[0].T, cloud_location_ind[1].T]
                    current_cloud.update_status(
                        time, cloud_values, lat[avg_lat_ind], lon[avg_lon_ind], lat_resolution, lon_resolution)
                else:
                    current_cloud.update_missing_cloud()


if __name__ == "__main__":
    fast_mode = False
    write_csv = True
    agg_fact = 3
    
    # 2,5,10,15,38
    t_deltas = [5]
    min_temp_array, max_temp_array = generate_temp_range(t_deltas)
    cloud_list = []
    # TODO: Paralelize here
    for temp_ind in range(len(min_temp_array)):
        analize_single_temp_range()
    cloud_list_agg.append(cloud_list)
