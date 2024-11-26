# from toolz import pipe
# import xarray as xr
from datetime import datetime, timedelta
import numpy as np
import os
from dateutil.relativedelta import relativedelta
from pandas import date_range
# Specify the folder path
# TODO: Make it extract an env variable
global CLAAS_FP
CLAAS_FP = "/cluster/work/climate/dnikolo/CLAAS_Data/"


def round_time(tm, round_increment):
    """Rounds down the datetime `tm` down to the nearest `round_increment` in minutes."""
    return tm - timedelta(minutes=tm.minute % round_increment,
                          seconds=tm.second,
                          microseconds=tm.microsecond)


def generate_month_folder_names(start_time, end_time, round_increment=15):
    """
    Generates a list of month folder names based on time interval.

    Parameters:
        start_time (datetime): Start time.
        end_time (datetime): End time.
        round_increment (int): Time interval in minutes.

    Returns:
        array: List of folder paths for each time interval.
    """
    # Round start and end times
    start_time = round_time(start_time, round_increment)
    end_time = round_time(end_time, round_increment)
    folder_format = "%Y_%m"
    month_folder_names = [start_time.strftime(folder_format)]
    month_folder_names.extend(date_range(start_time, end_time,
                                         freq='MS').strftime(folder_format).to_list())
    # month_folder_names=np.array([os.path.join(t.strftime("%Y_%m")) for t in time_range])
    return set(month_folder_names)


def first_day_of_month(month_str):
    return datetime.strptime(month_str, "%Y_%m").replace(day=1, hour=0, minute=0, second=0)


def last_day_of_month(month_str):
    any_day = datetime.strptime(month_str, "%Y_%m")
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return (next_month - timedelta(days=next_month.day)).replace(hour=23, minute=59, second=59)


def gen_filename_list(start_time, end_time):
    folder_format = "%Y_%m"
    file_format = folder_format+"/CPH/CPPin%Y%m%d%H%M%S.nc"
    file_names = [start_time.strftime(file_format)]
    file_names.extend(date_range(start_time, end_time,
                                 freq=timedelta(minutes=15)).strftime(file_format).to_list())
    return file_names


def generate_file_names(start_time, end_time, target_months, round_increment=15):
    """
    Generates a list of folder paths based on time intervals.

    Parameters:
        start_time (datetime): Start time.
        end_time (datetime): End time.
        round_increment (int): Time interval in minutes.

    Returns:
        list: List of folder paths for each time interval.
    """
    # Round start and end times
    start_time = round_time(start_time, round_increment)
    end_time = round_time(end_time, round_increment)
    folder_format = "%Y_%m"
    file_format = folder_format+"/CPH/CPPin%Y%m%d%H%M%S.nc"
    # Generate array of time incremented times
    file_names = []
    for month in target_months:
        if month == start_time.strftime(folder_format):
            month_end = last_day_of_month(month)
            if end_time < month_end:
                file_names.extend(gen_filename_list(start_time, end_time))
            else:
                file_names.extend(gen_filename_list(start_time, month_end))
        elif month == end_time.strftime(folder_format):
            month_start = first_day_of_month(month)
            file_names.extend(gen_filename_list(month_start, end_time))
        else:
            month_start = first_day_of_month(month)
            month_end = last_day_of_month(month)
            file_names.extend(gen_filename_list(month_start, month_end))
    # Generate folder paths

    return set(file_names)


if __name__ == "__main__":
    # Test case
    start_time = datetime(2004, 1, 2, 12, 5)  # Example start time
    end_time = datetime(2004, 3, 1, 14, 5)   # Example end time
    # Generate the name of the folders of each required month
    month_folder_names = generate_month_folder_names(start_time, end_time)
    print(month_folder_names)
    unprocessed_folders = set(os.listdir(
        os.path.join(CLAAS_FP, "Unprocessed_data/")))
    resampled_folders = set(os.listdir(
        os.path.join(CLAAS_FP, "Resampled_data/")))
    print(unprocessed_folders)
    unprocessed_target_months = month_folder_names-resampled_folders
    assert unprocessed_target_months.issubset(
        unprocessed_folders), f"One of the month folders doesn't exist in the downloaded dataset \nMissing months={unprocessed_target_months-unprocessed_folders}"
    print(unprocessed_target_months)
    target_unprocessed_file_names = generate_file_names(
        start_time, end_time, unprocessed_target_months)
    target_unprocessed_file_names_set = set(generate_file_names(
        start_time, end_time, unprocessed_target_months))
    filename_list = []
    print(len(target_unprocessed_file_names_set))
    for month in unprocessed_target_months:
        folder_contained_files = set([month+"/CPH/"+filename[:19]+".nc" for filename in os.listdir(
            os.path.join(CLAAS_FP, "Unprocessed_data/", month, "CPH")) if filename.startswith("CPPin")])
        target_unprocessed_file_names_set = target_unprocessed_file_names_set - \
            folder_contained_files
        # filename_check=[filename[:19]+".nc" for filename in filename_list]
    assert len(
        target_unprocessed_file_names_set) == 0, f"All month folders are present but some files don't exist in the downloaded dataset \nNumber of missing files = {len(target_unprocessed_file_names_set)}\nMissing files={target_unprocessed_file_names_set}"

    
    # target_file_names = generate_file_names(start_time, end_time)
    # existing_files=np.array(os.listdir( os.path.join(CLAAS_FP, "Unprocessed_data/")))
    # existing_files=os.listdir(glob.glob(CLAAS_FP+ '/Unprocessed_data/CPH'+'/*'))
    # existing_files=glob.glob(CLAAS_FP+ '/Unprocessed_data/CPH'+'/*')
    # # comparison_names=existing_files[:][:19]+".nc"

# Loop through the files in the folder
# for filename in os.listdir(folder_path):
#     # Check if the file starts with 'CPPin'
#     if filename.startswith("CPPin"):
#         # Full path of the file
#         new_filename=filename[:19]+".nc"
#         file_path = os.path.join(folder_path, filename)
#         print(f"Processing file: {file_path}")
# xr.load_dataset("/cluster/work/climate/dnikolo/CLAAS_Data/sample_2024/CPH/ORD57534/CPPin20241001000000405SVMSGI1UD.nc")
