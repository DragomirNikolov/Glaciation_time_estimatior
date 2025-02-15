import subprocess
import sys
import os
import tempfile
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Glaciation_time_estimator.Data_preprocessing.File_name_generator import generate_filename_dict
from Glaciation_time_estimator.Auxiliary_func.config_reader import read_config
import numpy as np
import argparse

def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description="Create a custom PyFLEXTRKR config file from terminal."
    )
    parser.add_argument('-t', "--temperature_bounds", nargs=2,
                        help="Min temp and max temp of file for analysis", type=int, required=True)
    parser.add_argument('-p', "--pole_folder",
                        help="Name of pole folder", required=True)
    args, _ = parser.parse_known_args()
    return {
        'temp_bounds': args.temperature_bounds,
        'pole': args.pole_folder
    }

def generate_remote_fps(config, cmd_args):
    target_dict = generate_filename_dict(exclude_existing=False)
    # Replace filename parts to include temperature bounds.
    target_fps = np.char.replace(
        target_dict[cmd_args["pole"]]["filter"],
        f"Agg_{config['agg_fact']:02}_",
        f"Agg_{config['agg_fact']:02}_T_{abs(cmd_args['temp_bounds'][0]):02}_{abs(cmd_args['temp_bounds'][1]):02}_"
    )
    return np.char.replace(target_fps, "Resampled_Data", "Filtered_Data")

if __name__ == "__main__":
    config = read_config()
    target_fps = generate_remote_fps(config, parse_cmd_args())

    # Remote host specification.
    remote_host = "user@remote"  # Change this to your actual remote host (and username if needed).

    # Create the local destination directory.
    tmp_dir = os.environ["TMPDIR"]
    data_dir = os.path.join(tmp_dir, "Data")
    os.makedirs(data_dir, exist_ok=True)
    print(f"Copying needed files from {remote_host} to {data_dir}")

    # Process file list:
    # The generated paths might already include the remote host prefix.
    # Remove the remote host (and the colon) so that paths are absolute on the remote host.
    files_to_copy = []
    for fp in target_fps:
        fp = str(fp)
        prefix = f"{remote_host}:"
        if fp.startswith(prefix):
            fp = fp[len(prefix):]
        files_to_copy.append(fp)

    if not files_to_copy:
        print("No files to copy.")
        sys.exit(0)

    # Write the file list to a temporary file.
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        tmp_filename = f.name
        for path in files_to_copy:
            f.write(path + "\n")

    # Use rsync's --files-from option.
    # The source directory is set to "/" so that the file paths in the list (which are absolute)
    # are interpreted correctly on the remote machine.
    cmd = [
        "rsync", "-auq",
        f"--files-from={tmp_filename}",
        f"{remote_host}:/",  # Source: remote host's root directory.
        data_dir           # Destination: local data_dir.
    ]
    print("Running rsync with --files-from")
    subprocess.run(cmd)

    # Remove the temporary file.
    os.remove(tmp_filename)
