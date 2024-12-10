
import subprocess
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Data_preprocessing.File_name_generator import generate_filename_dict
from datetime import datetime
import numpy as np
import argparse




def parse_cmd_args():
    # Retrieve cmd arguments
    parser = argparse.ArgumentParser(
        description="Create a custom PyFLEXTRKR config file from terminal."
    )
    parser.add_argument('-t', "--temperature_bounds", nargs=2,
                        help="Min temp and max temp of file for analysis", type=int, required=True)
    parser.add_argument('-st', "--start_time",
                        help="Start time in format YYYYMMDDHHMM", required=True)
    parser.add_argument('-et', "--end_time",
                        help="End time in format YYYYMMDDHHMM", required=True)
    parser.add_argument('-af', "--aggregation_factor",
                        help="Aggregation factor of file in use", type=int, required=True)
    parser.add_argument('-p', "--pole_folder",
                        help="Name of pole folder", required=True)

    # parser.add_argument("-wd", "--work_directory", help="Base yaml config file on which to draw upon", required=True)
    args = parser.parse_args()

    # Put arguments in a dictionary
    args_dict = {
        'temp_bounds': args.temperature_bounds,
        'agg_fact': args.aggregation_factor,
        'start_time':   datetime.strptime(args.start_time, "%Y%m%d%H%M"),
        'end_time':   datetime.strptime(args.end_time, "%Y%m%d%H%M"),
        'pole': args.pole_folder
    }
    print(args_dict["start_time"])
    print(args_dict["end_time"])
    assert args_dict["start_time"] < args_dict["end_time"], (
        "End time shoule be after start time ")
    return args_dict


def generate_remote_fps(args_dict, t_deltas):
    target_dict = generate_filename_dict(
        start_time=args_dict["start_time"], end_time=args_dict["end_time"], t_deltas=t_deltas, agg_fact=args_dict["agg_fact"])
    target_fps = np.char.replace(
        target_dict[args_dict["pole"]]["filter"],
        f"Agg_{args_dict['agg_fact']:02}_",
        f"Agg_{args_dict['agg_fact']:02}_T_{abs(args_dict['temp_bounds'][0]):02}_{abs(args_dict['temp_bounds'][1]):02}_"
    )
    return np.char.replace(target_fps, "Resampled_Data", "Filtered_Data")


if __name__ == "__main__":
    args_dict = parse_cmd_args()
    t_deltas = [5]
    target_fps = generate_remote_fps(args_dict, t_deltas)
    vec_dirname=np.vectorize(os.path.dirname)
    target_folders =np.unique(vec_dirname(target_fps))
    tmp_dir = os.environ["TMPDIR"]
    data_dir= os.path.join(tmp_dir, "Data", "")
    os.makedirs(data_dir, exist_ok=True)
    print(data_dir)
    print(target_folders)
    for folder in target_folders:
        subprocess.run(["rsync", "-auq", f"n2o:{os.path.join(folder,'')}", data_dir ])

    # subprocess.run("scp", )
