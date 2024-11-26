# from toolz import pipe
# import xarray as xr
from datetime import datetime, timedelta
import numpy as np
import os
from dateutil.relativedelta import relativedelta
from pandas import date_range
# Specify the folder path
#TODO: Make it extract an env variable
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
    month_folder_names = date_range(start_time,end_time, 
              freq='MS').strftime("%Y_%m").to_list()
    month_folder_names.append(start_time.strftime("%Y_%m"))
    month_folder_names=np.array(month_folder_names)
    # month_folder_names=np.array([os.path.join(t.strftime("%Y_%m")) for t in time_range])
    return month_folder_names

def generate_file_names(start_time, end_time, round_increment=15):
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
    
    # Generate array of time incremented times
    time_range = np.arange(start_time, end_time, timedelta(minutes=round_increment)).astype(datetime)
    # Generate folder paths
    file_names = np.array([os.path.join(t.strftime("%Y_%m"),"CPPin"+t.strftime("%Y%m%d_%H%M%S")+".nc") for t in time_range])
    
    return file_names 

if __name__ == "__main__":
    # Test case
    start_time = datetime(2004, 1, 10, 12, 5)  # Example start time
    end_time = datetime(2024, 10, 1, 14, 5)   # Example end time
    month_folder_names=generate_month_folder_names(start_time, end_time)
    print(month_folder_names)
    unprocessed_folders_list=os.listdir( os.path.join(CLAAS_FP, "Unprocessed_data/"))
    resampled_folders_list=os.listdir( os.path.join(CLAAS_FP, "Resampled_data/"))
    combined_folders_list=unprocessed_folders_list+resampled_folders_list
    assert month_folder_names.issubset(combinde_folder_list), f"One of the month folders doesn't exist in the dataset \nSearched folder names: {month_folder_names} \nExisting folder names: {combined_folders_list}"
    print("Resampled and searched for folders intersection:",np.intersect1d(month_folder_names, np.array(resampled_folders_list)))
    print("Unprocessed and searched for folders intersection:",np.intersect1d(month_folder_names, np.array(unprocessed_folders_list)))
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
