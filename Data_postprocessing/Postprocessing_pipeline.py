import numpy as np
import xarray as xr
import numba as nb
from datetime import datetime
from Single_cloud_analysis import Cloud
from Glaciation_time_estimator.Auxiliary_func.config_reader import read_config
from Glaciation_time_estimator.Data_postprocessing.Job_result_fp_generator import generate_tracking_filenames
from multiprocessing import Manager
from Glaciation_time_estimator.Auxiliary_func.Nestable_multiprocessing import NestablePool
from functools import partial


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


def analize_single_temp_range(temp_ind: int, cloud_dict, tracking_fps, pole: str, config: dict) -> None:
    # loop_start_time=dt.datetime.now()
    min_temp, max_temp = config['min_temp_array'][temp_ind], config['max_temp_array'][temp_ind]
    # Load datasets
    temp_key = f'{abs(round(min_temp))}-{abs(round(max_temp))}'
    try:
        # current_iteration_dir = job_output_dir
        cloudtrack_data = xr.load_dataset(tracking_fps[pole][temp_key][0])
        trackstats_data = xr.load_dataset(
            tracking_fps[pole]["trackstats_final"])
        tracknumbers_data = xr.load_dataset(tracking_fps[pole]["tracknumbers"])
    except:  # Exception as inst:
        print(f"Skipping {min_temp} to {max_temp}")
        cloud_dict[temp_key] = np.array([])
        return None
    # Load relevant data from datasets into local variables
    n_tracks = trackstats_data.variables['track_duration'].shape[0]
    basetimes = tracknumbers_data['basetimes']
    lat = cloudtrack_data['lat']
    lon = cloudtrack_data['lon']
    lat_resolution = (lat.max()-lat.min())/len(lat)
    lon_resolution = (lon.max()-lon.min())/len(lon)
    trackstats_data.close()
    tracknumbers_data.close()
    cloudtrack_data.close()
    # print(append_start_time-loop_start_time)
    cloud_arr = np.empty((n_tracks), dtype=Cloud)
    # Cloud(f'{temp_ind}_{i}') for i in range(n_tracks)])
    # print(append_end_time-append_start_time)
    print(f'Analyzing T: {min_temp} to {max_temp} Agg={config['agg_fact']}')

    for fp_ind in range(len(basetimes)):
        time = datetime.utcfromtimestamp(basetimes[fp_ind])
        time_str = time.strftime("%Y%m%d_%H%M%S")
        print(f'{min_temp} to {max_temp} Loading {time_str}')
        cloudtrack_fp = tracking_fps[pole]['cloudtracks'][fp_ind]
        cloudtrack_data = xr.load_dataset(cloudtrack_fp)
        cloudtracknumber_field = cloudtrack_data['tracknumber'].data
        cph_field = cloudtrack_data['cph']
        cloud_id_in_field, counts = np.unique(
            cloudtracknumber_field, return_counts=True)
        counts = counts[cloud_id_in_field != 0]
        cloud_id_in_field = cloud_id_in_field[cloud_id_in_field != 0]
        max_allowed_cloud_size_px = config['fast_mode_arr_size'] if config['postprocessing_fast_mode'] else counts.max(
        )
        hash_map_cloud_numbers = extract_cloud_coordinates(
            cloudtracknumber_field, cloud_id_in_field, max_allowed_cloud_size_px)  # counts.max())
        cloudtrack_data.close()
        if max_allowed_cloud_size_px > 1000000:
            print(np.where(counts, counts == counts.max()))
        for track_number in cloud_id_in_field:
            try:
                if cloud_arr[track_number-1] is None:
                    cloud_arr[track_number-1] = Cloud(temp_key)
            except:
                print(
                    f"Error: {temp_ind,track_number,len(cloud_arr)}")
                continue

            if (not cloud_arr[track_number-1].terminate_cloud):
                # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
                ind, cord = hash_map_cloud_numbers[track_number]
                cloud_location_ind = [cord[0, :ind], cord[1, :ind]]
                if cloud_location_ind[0].size != 0:
                    avg_lat_ind = int(round(np.mean(cloud_location_ind[0])))
                    avg_lon_ind = int(round(np.mean(cloud_location_ind[1])))
                    # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
                    cloud_values = cph_field[0,
                                             cloud_location_ind[0].T, cloud_location_ind[1].T]
                    cloud_arr[track_number-1].update_status(
                        time, cloud_values, lat[avg_lat_ind], lon[avg_lon_ind], lat_resolution, lon_resolution)
                else:
                    cloud_arr[track_number-1].update_missing_cloud()
    cloud_dict[temp_key] = cloud_arr
# def analyze_basetime():


def analize_single_pole(pole, cloud_dict, tracking_fps, config, n_procs=4):
    with NestablePool(n_procs) as pool:
        part_single_temp_range = partial(
            analize_single_temp_range, cloud_dict=cloud_dict, tracking_fps=tracking_fps, pole=pole, config=config)
        pool.map(part_single_temp_range, range(len(config['min_temp_array'])))
        pool.close()


if __name__ == "__main__":
    config = read_config()
    tracking_fps = generate_tracking_filenames(config)
    with Manager() as manager:
        cloud_dict = manager.dict()
        # TODO: Paralelize here
        part_analize_single_pole = partial(
            analize_single_pole, cloud_dict=cloud_dict, tracking_fps=tracking_fps, config=config)
        with NestablePool(2) as pool:
            pool.map(part_analize_single_pole, config['pole_folders'])
            pool.close()
        print(cloud_dict["5_0"])
        #TODO: Saving
