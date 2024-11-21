import numpy as np
import netCDF4 as nc
import matplotlib.pyplot as plt
import time
from Helper_fun import generate_temp_range
import os
import datetime as dt
import math
from multiprocessing import Pool
from functools import partial


class cloud:
    # def __new__(self, *args, **kwargs):
    #     return super().__new__(self)
    def __init__(self, cloud_id):
        self.id = cloud_id
        self.crit_fraction = 0.1
        # Bools inidicating if the cloud has been liquid at any point
        self.is_liq: bool = False
        self.is_mix: bool = False
        self.is_ice: bool = False
        # Max and min cloud size in pixels
        self.max_size: int = 0
        self.min_size: int = 0

        self.max_water_fraction: float = 0.0
        self.max_ice_fraction: float = 0.0

        self.track_start_time: dt.datetime = None
        self.track_end_time: dt.datetime = None
        self.track_length = None

        self.glaciation_start_time: dt.datetime = None
        self.glaciation_end_time: dt.datetime = None

    def __str__(self):
        return f"{self.is_liq},{self.is_mix},{self.is_ice},"

    def update_status(self, time: dt.datetime, cloud_values: np.array):
        cloud_size = cloud_values.shape[0]
        if cloud_size:

            # I assume that water_frac+ice_frac=1
            water_fraction = float(np.count_nonzero(
                cloud_values == 1))/float(cloud_size)
            ice_fraction = float(np.count_nonzero(
                cloud_values == 2))/float(cloud_size)
            assert math.isclose(water_fraction+ice_fraction, 1)
            # print(water_fraction)
            # print(water_fraction)
            if not (self.track_start_time):
                self.track_start_time = time

            # Check and set type of cloud
            if water_fraction > 1-self.crit_fraction:
                self.is_liq = True
            elif water_fraction > self.crit_fraction:
                self.is_mix = True
            else:
                self.is_ice = True

            self.max_size = max(self.max_size, cloud_size)
            self.max_water_fraction = max(
                self.max_water_fraction, water_fraction)
            self.max_ice_fraction = max(
                self.max_ice_fraction, 1-water_fraction)
        else:
            if (not (self.track_end_time)) and self.track_start_time:
                self.track_end_time = time
                self.track_length = self.track_end_time-self.track_start_time


def analyze_current_cloud(track_number, temp_ind, cph_field, cloudtracknumber_field, cloud_list):
    try:
        current_cloud = cloud_list[temp_ind][track_number]
    except:
        # print(f"Error: {temp_ind,track_number,len(cloud_list)}")
        exit
    # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
    cloud_values = cph_field[cloudtracknumber_field == track_number+1]
    current_cloud.update_status(time, cloud_values)


def time_field_analysis(unix_time, cloud_list, n_tracks, min_temp, max_temp, job_output_dir):
    time = dt.datetime.utcfromtimestamp(unix_time)
    time_str = time.strftime("%Y%m%d_%H%M%S")
    # print(f'{min_temp} to {max_temp} Loading {time_str}')
    cloudtrack_fp = job_output_dir + \
        f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}/pixel_path_tracking/20040201.1415_20040201.2000/cloudtracks_{time_str}.nc'
    cloudtrack_data = nc.Dataset(cloudtrack_fp)
    cloudtracknumber_field = cloudtrack_data.variables['cloudtracknumber'][:, :, :]
    cph_field = cloudtrack_data.variables['cph'][:, :, :]

    for track_number in range(n_tracks):
        try:
            current_cloud = cloud_list[temp_ind][track_number]
        except:
            # print(f"Error: {temp_ind,track_number,len(cloud_list)}")
            exit
        # TODO:SPEED UP NEXT TWO LINES (set_cloud_values and update_status)
        cloud_values = cph_field[cloudtracknumber_field == track_number+1]
        current_cloud.update_status(time, cloud_values)


if os.environ['WORK_DIR'] != '':
    WORK_DIR = os.environ['WORK_DIR']
    PyFLEXTRKR_LIB_DIR = os.environ['PYFLEXTRKR_LIB_DIR']
else:
    raise ValueError(
        "PyFLEXTRKR_LIB_DIR environmental variable is empty or doesn't exist")
min_temp = -35
max_temp = -30


TMPDIR = os.environ['TMPDIR']
# '/Old_results_storage/Run12.11_all_t_ranges
job_output_dir = TMPDIR+'/Job_output'
# job_output_dir=WORK_DIR+'/Job_output'#'/Old_results_storage/Run12.11_all_t_ranges'

# 2,5,10,15,38
t_deltas = [5]
min_temp_array, max_temp_array = generate_temp_range(t_deltas)

agg_fact = 2
# print(dt.datetime.now())
if __name__ == "__main__":
    with Pool(os.cpu_count()) as pool:
        cloud_list = []

        pool = Pool(os.cpu_count())
        for temp_ind in range(len(min_temp_array)):

            # loop_start_time=dt.datetime.now()
            min_temp, max_temp = min_temp_array[temp_ind], max_temp_array[temp_ind]
            # Load datasets
            try:
                cloudtrack_fp = job_output_dir + \
                    f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}/pixel_path_tracking/20040201.1415_20040201.2000/cloudtracks_20040201_183000.nc'
                cloudtrack_data = nc.Dataset(cloudtrack_fp)
                if agg_fact != 0:
                    trackstats_data = nc.Dataset(
                        job_output_dir+f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}-agg-{agg_fact}/stats/trackstats_final_20040201.1415_20040201.2000.nc')
                    tracknumbers_data = nc.Dataset(
                        job_output_dir+f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}-agg-{agg_fact}/stats/tracknumbers_20040201.1415_20040201.2000.nc')
                else:
                    trackstats_data = nc.Dataset(
                        job_output_dir+f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}/stats/trackstats_final_20040201.1415_20040201.2000.nc')
                    tracknumbers_data = nc.Dataset(
                        job_output_dir+f'/T-{abs(round(min_temp))}-{abs(round(max_temp))}/stats/tracknumbers_20040201.1415_20040201.2000.nc')
            except:  # Exception as inst:
                # print(f"Skipping {min_temp} to {max_temp}")
                # #print(type(inst))    # the exception type

                # #print(inst.args)     # arguments stored in .args

                # #print(inst)          # __str__ allows args to be printed directly,

                # but may be overridden in exception subclasses
                cloud_list.append([])
                continue

            # Load relevant data from datasets into local variables
            cloudtracknumber_field = cloudtrack_data.variables['cloudtracknumber'][:, :, :]
            cph_field = cloudtrack_data.variables['cph'][:, :, :]
            n_tracks = trackstats_data.variables['track_duration'].shape[0]
            basetimes = tracknumbers_data.variables['basetimes'][:]
            n_tracks = trackstats_data.variables['track_duration'].shape[0]

            # FIX CLOUD TIMES
            # print(n_tracks)
            # append_start_time=dt.datetime.now()]
            cloud_list.append([cloud(f'{temp_ind}_{i}')
                              for i in range(n_tracks)])

            partial_function = partial(time_field_analysis, cloud_list=cloud_list, n_tracks=n_tracks,
                                       min_temp=min_temp, max_temp=max_temp, job_output_dir=job_output_dir)
            pool.map(partial_function, basetimes)
            # Initialize the pool and map the partial function
