import yaml
import argparse
import os
from datetime import datetime
from Glaciation_time_estimator.Auxiliary_func.Helper_fun import generate_temp_range


def parse_cmd_args():
    # Retrieve cmd arguments
    parser = argparse.ArgumentParser(
        description="Create a custom PyFLEXTRKR config file from terminal."
    )
    parser.add_argument('-cf', "--configuration_filepath",
                        help="Path to config function", required=True)

    # parser.add_argument("-wd", "--work_directory", help="Base yaml config file on which to draw upon", required=True)
    # args = parser.parse_args()
    args,_ = parser.parse_known_args()
    assert os.path.exists(
        args.configuration_filepath), "Configuration file doesn't exist"
    return args.configuration_filepath


def format_config(config):
    date_format = "%Y%m%d_%H%M"
    config["start_time"] = datetime.strptime(config["start_time"], date_format)
    config["end_time"] = datetime.strptime(config["end_time"], date_format)
    assert config["start_time"]<config["end_time"], "Start time should be before end time"
    config["struct_boundary_date"] = datetime.strptime(
        config["struct_boundary_date"], date_format)
    min_temp_arr, max_temp_arr = generate_temp_range(config["t_deltas"])
    config["min_temp_arr"] = min_temp_arr
    config["max_temp_arr"] = max_temp_arr
    return config


def read_config():
    config_fp = parse_cmd_args()
    with open(config_fp) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    config = format_config(config)
    return config


if __name__ == "__main__":
    print(read_config())
