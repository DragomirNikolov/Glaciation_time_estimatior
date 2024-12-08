import os
import xarray as xr
from datetime import datetime
import numpy as np
from Resample_data import ProjectionTransformer
from Temp_filter import TempFilter
from File_name_generator import generate_filename_dict
from Output_file_generation import OutputFilteredFile, OutputResampledFile
import subprocess
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


def generate_output(target_filenames):
    pole_folders = ["np", "sp"]
    aux_fps = {"np": "/wolke_scratch/dnikolo/CLAAS_Data/np/CM_SAF_CLAAS3_L2_AUX.nc",
               "sp": "/wolke_scratch/dnikolo/CLAAS_Data/sp/CM_SAF_CLAAS3_L2_AUX.nc"}
    for pole in pole_folders:
        transformer = ProjectionTransformer()
        aux_data = xr.load_dataset(os.path.join(
            CLAAS_FP, aux_fps[pole]), decode_times=False)
        transformer.generate_lat_lon_prj(aux_data)
        for filename_ind in range(len(target_filenames[pole]["resample_CPP"])):
            # xr.load_mfdataset(filenames_it)
            # Load new dataset
            cpp_filename = target_filenames[pole]["resample_CPP"][filename_ind]
            ctx_filename = target_filenames[pole]["resample_CTX"][filename_ind]

            # Open relevant datasets
            input_cpp_ds = xr.load_dataset(cpp_filename)
            input_ctx_ds = xr.load_dataset(ctx_filename)

            # Resample dataset contents
            resampled_cph_data = transformer.remap_data(
                input_cpp_ds["cph"].data)
            resampled_ctt_data = transformer.remap_data(
                input_ctx_ds["ctt"].data)
            resampled_cth_data = transformer.remap_data(
                input_ctx_ds["cth"].data)
            print(f"Resampled: {cpp_filename} and {ctx_filename}")
            # Generate output file and save result
            output_file = OutputResampledFile(
                input_cpp_ds, input_ctx_ds, agg_fact=1)
            output_file.add_coords(transformer.new_cord_lat,
                                   transformer.new_cord_lon)
            output_file.set_output_variables(
                resampled_cph_data, resampled_ctt_data, resampled_cth_data)
            resample_res_fp = target_filenames[pole]["resample_res"][filename_ind]
            agg_res_fp = target_filenames[pole]["agg_res"][filename_ind]
            output_file.save_file(resample_res_fp)
            # Close dataset
            input_cpp_ds.close()
            subprocess.run(["cdo", f"gridboxmean,{agg_fact},{agg_fact}",
                           "-selname,cph_resampled,ctt_resampled", resample_res_fp, agg_res_fp])

        aux_data.close()


def generate_filtered_output_fps(day_fp, agg_fact, min_temp, max_temp):
    output_fp = day_fp
    output_fp = np.char.replace(output_fp, "Resampled_Data", "Filtered_Data")
    output_fp = np.char.replace(
        output_fp, f"Agg_{agg_fact:02}", f"Agg_{agg_fact:02}_T_{abs(min_temp)}_{abs(max_temp)}")
    os.makedirs(os.path.dirname(output_fp[0]), exist_ok=True)
    return output_fp


def generate__filtered_files(target_filenames, t_deltas, agg_fact):
    pole_folders = ["np", "sp"]
    # aux_fps = {"np": "/wolke_scratch/dnikolo/CLAAS_Data/np/CM_SAF_CLAAS3_L2_AUX.nc" , "sp": "/wolke_scratch/dnikolo/CLAAS_Data/sp/CM_SAF_CLAAS3_L2_AUX.nc"}
    temp_bounds = generate_temp_range(t_deltas)
    for pole in pole_folders:
        # print("a")
        # filter = TempFilter(t_deltas)
        fp_to_filter = target_filenames[pole]["filter"]
        vec_dirname = np.vectorize(os.path.dirname)
        # print(vec_dirname(fp_to_filter))
        _ , folder_start_ind = np.unique(
            vec_dirname(fp_to_filter), return_index=True)

        for ind in range(len(folder_start_ind)):
            # print("b")
            if ind < len(folder_start_ind)-1:
                start_ind = folder_start_ind[ind]
                end_ind = folder_start_ind[ind+1]
                day_fp_to_filter = fp_to_filter[start_ind:end_ind]
                # print(start_ind, end_ind)
            else:
                start_ind = folder_start_ind[ind]
                day_fp_to_filter = fp_to_filter[start_ind:]
                # print(start_ind)

            # print(day_fp_to_filter)
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
            # for cph_filtered in filter.filter_data(resampled_ds):
            #     OutputFilteredFile()

            #     print(f"Resampled: {cpp_filename} and {ctx_filename}")
            #     # Generate output file and save result
            #     output_file = OutputResampledFile(
            #         input_cpp_ds, input_ctx_ds, 1)
            #     output_file.add_coords(transformer.new_cord_lat,
            #                            transformer.new_cord_lon)
            #     output_file.set_output_variables(
            #         resampled_cph_data, resampled_ctt_data, resampled_cth_data)
            #     output_file.save_file(
            #         target_filenames[pole]["resample_res"][filename_ind])
            #     # Close dataset
            #     input_cpp_ds.close()


if __name__ == "__main__":
    print("Generating target filenames")
    t_deltas = [5]
    agg_fact = 3
    target_filenames = generate_filename_dict(start_time=datetime(
        2023, 1, 8, 0, 15), end_time=datetime(2023, 1, 15, 0, 0), t_deltas=t_deltas, agg_fact=agg_fact)

    print(target_filenames)
    print("Target filenames generated")
    print("Resampling needed files")
    generate_output(target_filenames)
    generate__filtered_files(target_filenames, t_deltas, agg_fact=agg_fact)
    print("Needed files resampled")
