
import subprocess
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Glaciation_time_estimator.Data_preprocessing.File_name_generator import generate_filename_dict
from Glaciation_time_estimator.Auxiliary_func.config_reader import read_config
from datetime import datetime
import numpy as np
import argparse


def generate_remote_fps(pole):
    target_dict = generate_filename_dict(exclude_existing=False)
    return target_dict[pole]["resample_CPP"]


if __name__ == "__main__":
    config = read_config()
    tmp_dir = os.environ["TMPDIR"]
    vec_dirname = np.vectorize(os.path.dirname)
    for pole in config["pole_folders"]:
        target_fps = generate_remote_fps(pole)
        target_folders =np.unique(vec_dirname(target_fps))
        data_dir= os.path.join(tmp_dir, "Data",pole, "")
        os.makedirs(data_dir, exist_ok=True)
        print(target_folders)
        print(data_dir)
        print(config["postprocessing_output_dir"], config["CLAAS_fp"])
        for folder in target_folders:
            subprocess.run(["rsync", "-auq", f"{os.path.join(folder,'')}", data_dir ])

    # subprocess.run("scp", )
