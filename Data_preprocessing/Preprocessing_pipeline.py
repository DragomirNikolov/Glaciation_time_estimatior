import os
import xarray as xr
from datetime import datetime
import numpy as np
import subprocess
import time
import threading
from Resample_data import ProjectionTransformer
from File_name_generator import generate_filename_dict
from Output_file_generation import OutputResampledFile

global CLAAS_FP
CLAAS_FP = os.environ["CLAAS_DIR"]


def generate_temp_range(t_deltas: list) -> tuple:
    boundary_temps = np.arange(0, -38.1, -t_deltas[0])
    t_min = boundary_temps[1:].astype(int)
    t_max = boundary_temps[0:-1].astype(int)
    for i in range(len(t_deltas)-1):
        boundary_temps = np.arange(0, -38.1, -t_deltas[i+1])
        # t_ranges_temp=np.concatenate((boundary_temps[1:][None,:].astype(int),boundary_temps[0:-1][None,:].astype(int)),0,dtype='int')
        t_min = np.concatenate((t_min, boundary_temps[1:].astype(int)))
        t_max = np.concatenate((t_max, boundary_temps[0:-1].astype(int)))
    return t_min, t_max


def fps_by_folder(fp_arr_target):
    if len(fp_arr_target) != 0:
        vec_dirname = np.vectorize(os.path.dirname)
        _, folder_start_ind = np.unique(
            vec_dirname(fp_arr_target), return_index=True)
        fps_by_folder = []
        for ind in range(len(folder_start_ind)):
            # print("b")
            if ind < len(folder_start_ind)-1:
                start_ind = folder_start_ind[ind]
                end_ind = folder_start_ind[ind+1]
                fps_by_folder.append(fp_arr_target[start_ind:end_ind])
                # print(start_ind, end_ind)
            else:
                start_ind = folder_start_ind[ind]
                fps_by_folder.append(fp_arr_target[start_ind:])
        return fps_by_folder
    return []


def dispatch(sem, argv, **kw):
    try:
        subprocess.run(argv, **kw)
    finally:
        sem.release()


def aggregation(resample_res_fps, agg_res_fps):
    sem = threading.Semaphore(8)   # pick a threshold here

    Ts = []
    for filename_ind in range(len(resample_res_fps)):
        resample_res_file = resample_res_fps[filename_ind]
        agg_res_file = agg_res_fps[filename_ind]
        argv = ["cdo", f"gridboxmean,{agg_fact},{agg_fact}",
                "-selname,cph_resampled,ctt_resampled", resample_res_file, agg_res_file]
        sem.acquire()
        T = threading.Thread(target=dispatch, args=(sem, argv))
        T.start()
        Ts.append(T)

    for T in Ts:
        T.join()


def generate_resampled_output(target_filenames):
    pole_folders = ["np", "sp"]
    aux_fps = {"np": "/wolke_scratch/dnikolo/CLAAS_Data/np/CM_SAF_CLAAS3_L2_AUX.nc",
               "sp": "/wolke_scratch/dnikolo/CLAAS_Data/sp/CM_SAF_CLAAS3_L2_AUX.nc"}
    start_time = time.time()
    for pole in pole_folders:
        transformer = ProjectionTransformer()
        aux_data = xr.load_dataset(os.path.join(
            CLAAS_FP, aux_fps[pole]), decode_times=False)
        transformer.generate_lat_lon_prj(aux_data)
        # if len(target_filenames[pole]["resample_CTX"]) == 0:
        #     continue
        folder_fps_CTX = fps_by_folder(target_filenames[pole]["resample_CTX"])
        folder_fps_CPP = fps_by_folder(target_filenames[pole]["resample_CPP"])
        folder_resample_res_fps = fps_by_folder(
            target_filenames[pole]["resample_res"])
        folder_agg_res_fps = fps_by_folder(
            target_filenames[pole]["agg_res"])
        for folder_fp_ind in range(len(folder_fps_CTX)):
            day_start_time = time.time()
            # Open relevant datasets
            # print(len(folder_fps_CTX[folder_fp_ind]))
            # print(len(folder_fps_CPP[folder_fp_ind]))
            input_ctx_ds = xr.open_mfdataset(
                list(folder_fps_CTX[folder_fp_ind]), parallel=True, chunks={"time": len(folder_fps_CTX[folder_fp_ind]), "x": aux_data.sizes["x"], "y": aux_data.sizes["y"]})
            input_cpp_ds = xr.open_mfdataset(
                list(folder_fps_CPP[folder_fp_ind]), parallel=True, chunks={"time": len(folder_fps_CPP[folder_fp_ind]), "x": aux_data.sizes["x"], "y": aux_data.sizes["y"]})

            output_file = OutputResampledFile(
                input_cpp_ds, agg_fact=1)
            output_file.add_coords(transformer.new_cord_lat,
                                   transformer.new_cord_lon)
            # Resample cpx dataset contents
            resampled_ctt_data = transformer.remap_data(
                input_ctx_ds["ctt"])
            resampled_cth_data = transformer.remap_data(
                input_ctx_ds["cth"])
            output_file.set_ctx_output_variables(
                resampled_ctt_data, resampled_cth_data)
            # del resampled_ctt_data, resampled_cth_data
            input_ctx_ds.close()
            # Resample cpp dataset contents
            resampled_cph_data = transformer.remap_data(
                input_cpp_ds["cph"])
            output_file.set_cpp_output_variables(
                resampled_cph_data)
            # Generate output file and save result
            resample_res_fps = folder_resample_res_fps[folder_fp_ind]
            agg_res_fps = folder_agg_res_fps[folder_fp_ind]
            output_file.save_file(resample_res_fps)
            # Close dataset
            input_cpp_ds.close()
            resample_end_time = time.time()
            print(
                f"Resampled {pole} day {folder_fp_ind} in {round(resample_end_time-day_start_time,2)}s starting with fp: {folder_fps_CTX[folder_fp_ind][0]}")
            aggregation(resample_res_fps, agg_res_fps)
            agg_end_time = time.time()
            print(
                f"Aggregated {pole} day {folder_fp_ind} in {round(agg_end_time - resample_end_time,2)}s")
        aux_data.close()
    end_time = time.time()
    print(f"Total resampling + agg time = {round(end_time-start_time,2)}")


def generate_filtered_output_fps(day_fp, agg_fact, min_temp, max_temp):
    output_fp = day_fp
    output_fp = np.char.replace(output_fp, "Resampled_Data", "Filtered_Data")
    output_fp = np.char.replace(
        output_fp, f"Agg_{agg_fact:02}", f"Agg_{agg_fact:02}_T_{abs(min_temp):02}_{abs(max_temp):02}")
    os.makedirs(os.path.dirname(output_fp[0]), exist_ok=True)
    return output_fp


def generate_filtered_files(target_filenames, t_deltas, agg_fact):
    pole_folders = ["np", "sp"]
    temp_bounds = generate_temp_range(t_deltas)
    for pole in pole_folders:
        for day_fp_to_filter in fps_by_folder(target_filenames[pole]["filter"]):
            combined_ds = xr.open_mfdataset(
                list(day_fp_to_filter), parallel=True)
            for temp_ind in range(len(temp_bounds[0])):
                min_temp = temp_bounds[0][temp_ind]
                max_temp = temp_bounds[1][temp_ind]
                mask = (combined_ds['ctt_resampled'] >= 273.15 +
                        min_temp) & (combined_ds['ctt_resampled'] <= 273.15+max_temp)
                combined_ds['cph_filtered'] = xr.where(
                    mask, combined_ds['cph_resampled'], 0).compute()
                output_fps = generate_filtered_output_fps(
                    day_fp_to_filter, agg_fact, min_temp, max_temp)
                _, dataset_list = zip(
                    *(combined_ds.groupby("time")))
                xr.save_mfdataset(dataset_list, list(output_fps))


if __name__ == "__main__":
    print("Generating target filenames")
    t_deltas = [5]
    agg_fact = 3
    target_filenames = generate_filename_dict(start_time=datetime(
        2023, 1, 16, 0, 0), end_time=datetime(2023, 1, 31, 23, 45), t_deltas=t_deltas, agg_fact=agg_fact)
    # print(target_filenames)
    print("Target filenames generated")
    print("Resampling needed files")
    generate_resampled_output(target_filenames)
    print("Needed files resampled. Start filtering")
    generate_filtered_files(target_filenames, t_deltas, agg_fact=agg_fact)
<<<<<<< HEAD
    print("Needed files resampled")
=======
    print("Filtering complete")
>>>>>>> 13394e0cf9e8d177816d28ed2200ef8343a6da5b
