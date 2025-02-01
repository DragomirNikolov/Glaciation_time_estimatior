
import subprocess
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Glaciation_time_estimator.Data_preprocessing.File_name_generator import generate_filename_dict
from Glaciation_time_estimator.Auxiliary_func.config_reader import read_config
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
    parser.add_argument('-p', "--pole_folder",
                        help="Name of pole folder", required=True)

    # parser.add_argument("-wd", "--work_directory", help="Base yaml config file on which to draw upon", required=True)
    args,_ = parser.parse_known_args()

    # Put arguments in a dictionary
    args_dict = {
        'temp_bounds': args.temperature_bounds,
        'pole': args.pole_folder
        
    }
    return args_dict


def generate_remote_fps(config,cmd_args):
    target_dict = generate_filename_dict()
    target_fps = np.char.replace(
        target_dict[cmd_args["pole"]]["filter"],
        f"Agg_{config['agg_fact']:02}_",
        f"Agg_{config['agg_fact']:02}_T_{abs(cmd_args['temp_bounds'][0]):02}_{abs(cmd_args['temp_bounds'][1]):02}_"
    )
    return np.char.replace(target_fps, "Resampled_Data", "Filtered_Data")


if __name__ == "__main__":
    config = read_config()
    target_fps = generate_remote_fps(config,parse_cmd_args())
    vec_dirname=np.vectorize(os.path.dirname)
    target_folders =np.unique(vec_dirname(target_fps))
    tmp_dir = os.environ["TMPDIR"]
    data_dir= os.path.join(tmp_dir, "Data", "")
    os.makedirs(data_dir, exist_ok=True)
    print(data_dir)
    print(target_folders)
    for folder in target_folders:
        subprocess.run(["rsync", "-auq", f"{os.path.join(folder,'')}", data_dir ])

    # subprocess.run("scp", )
