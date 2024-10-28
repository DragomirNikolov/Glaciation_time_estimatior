#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filename: Config_generator.py
Author: Dragomir Nikolov    
Date: 2024-10-11  (YYYY-MO-DD)
Version: 1.0
Description: 
    This file is used to generate configuration files for the PyFLEXTRKR library within an existing job.
    This is neded to be able to place the output directory in $TEMPDIR

License: MIT License
Contact: dnikolo@student.ethz.ch / dragomird.nikolov@gmail.com
Dependencies: os, sys
"""

# Import statements
import yaml
import os
import argparse

def parse_cmd_args():
    # Retrieve cmd arguments
    parser = argparse.ArgumentParser(
        description="Create a custom PyFLEXTRKR config file from terminal."
    )
    parser.add_argument('-t',"--temperature_bounds" ,nargs=2, help="Min temp and max temp of file for analysis",type=int, required=True)
    parser.add_argument("-bc", "--base_configuration", help="Base yaml config file on which to draw upon", required=True)
    parser.add_argument('-af',"--aggregation_factor" , help="Aggregation factor of file in use",type=int, required=False)
    # parser.add_argument("-wd", "--work_directory", help="Base yaml config file on which to draw upon", required=True)
    args = parser.parse_args()

    # Put arguments in a dictionary
    args_dict = {
        'temp_bounds': args.temperature_bounds,
        'base_config': args.base_configuration,
        'agg_fact': args.aggregation_factor
    }

    return args_dict

def config_setup(work_dir: str, min_temp, max_temp, base_setup_contnet: list, agg_fact=None) -> str:
    """
    Writes configuratuin file

    Input:
    work_dir: Base directory in which the TEST and Glaciation_time_estimatior folders are
    min_temp: Minimum CTT of clouds after filtering
    max_temp: Maximum CTT of clouds after filtering
    base_setup_content: The contents of a setup file that will be used as a basis for the new one. 
                        This file should contain all the variables necessary for PyFLEXTRKR to run.
    Outout:
    temp_setup_fp: location of the newly created configuation file
    """
    temp_setup = base_setup_contnet
    # Set temp_setup names
    if agg_fact!=None:
        temp_setup["databasename"] = f"CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-agg_{round(agg_fact)}-"
        root_path=work_dir + f'/T-{round(abs(min_temp))}-{round(abs(max_temp))}-agg-{round(agg_fact)}'
        print(root_path)
    else:
        temp_setup["databasename"] = f"CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-"
        root_path=work_dir + f'/T-{round(abs(min_temp))}-{round(abs(max_temp))}'
    #Create root path folder if it doesn't exist
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    temp_setup['root_path'] = root_path
    temp_setup['logger_filepath'] = root_path+ '/log.log'
    temp_setup['error_filepath'] = root_path + '/err.log'
    temp_setup['pixel_radius']= temp_setup['pixel_radius']*agg_fact
    temp_setup_fp = root_path + '/setup.yml'
    with open(temp_setup_fp, 'w') as temp_setup_file:
        yaml.dump(temp_setup, temp_setup_file)
    return temp_setup_fp

if __name__=="__main__":
    base_dir = os.environ['TMPDIR']
    arg_dict=parse_cmd_args()
    
    with open(arg_dict['base_config'], 'r') as stream:
        try:
            custom_setup = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit
    print("The agg factor is: ",arg_dict['agg_fact'])
    config_setup(base_dir, abs(round(arg_dict['temp_bounds'][0])), abs(round(arg_dict['temp_bounds'][1])), custom_setup, agg_fact=arg_dict['agg_fact'])
    

