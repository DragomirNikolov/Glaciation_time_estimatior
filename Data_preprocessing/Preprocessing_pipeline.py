import os
import xarray as xr
from Resample_data import ProjectionTransformer
from Resampling_file_search import filenames_to_resample
from datetime import datetime

global CLAAS_FP
CLAAS_FP = "/cluster/work/climate/dnikolo/CLAAS_Data/"


class OutputFile:
    def __init__(self, cph_dataset, ctt_dataset, agg_fact):
        self.cph_ds = cph_dataset
        self.ctt_ds = ctt_dataset
        self.agg_fact = agg_fact

    def add_coords(self, lats, lons):
        self.cph_ds = self.cph_ds.assign_coords({"lon": lons, "lat": lats})
        self.cph_ds.lat.attrs = {"standard_name": "latitude",
                                 "long_name": "latitude",
                                 "units": "degrees_north", }
        self.cph_ds.lon.attrs = {"standard_name": "longitude",
                                 "long_name": "longitude",
                                 "units": "degrees_east", }

    def set_output_variables(self, resampled_cph_data, resampled_ctt_data, resampled_cth_data):
        self.cph_ds["cph_resampled"] = xr.DataArray(
            resampled_cph_data,
            dims=("time", "lat", "lon"),
            attrs={
                "cell_methods": "time: point",
                "flag_meanings": "clear liquid ice",
                "flag_values": "0s, 1s, 2s",
                "missing_value": -1,
                "grid_mapping": "projection",
                "units": "1",
                "long_name": "Cloud Thermodynamic Phase",
                "standard_name": "thermodynamic_phase_of_cloud_water_particles_at_cloud_top",
            }
        )
        self.cph_ds["ctt_resampled"] = xr.DataArray(
            resampled_ctt_data,
            dims=("time", "lat", "lon"),
            attrs={
                "_FillValue": -1,
                "units": "K",
                "valid_range": [0, 4060],
                "standard_name": "air_temperature_at_cloud_top",
                "long_name": "Cloud Top Temperature",
                "grid_mapping": "projection",
                "cell_methods": "time: point",
                "add_offset": 0.0,
                "scale_factor": 0.1
            }
        )
        self.cph_ds["cth_resampled"] = xr.DataArray(
            resampled_cth_data,
            dims=("time", "lat", "lon"),
            attrs={
                "_FillValue": -1,
                "units": "m",
                "valid_range": [0, 30000],
                "standard_name": "cloud_top_altitude",
                "long_name": "Cloud Top Height",
                "grid_mapping": "projection",
                "cell_methods": "time: point",
                "add_offset": 0.0,
                "scale_factor": 1.0
            }
        )


class OutputResampledFile(OutputFile):
    def generate_output_fp(self, filename):
        split_filename = filename.split(os.path.sep)
        split_filename[-1] = f"Comb_agg_{self.agg_fact:02}_" + \
            split_filename[-1][5:]
        print(split_filename)
        output_folder = os.path.join(
            CLAAS_FP, "Resampled_data", *(split_filename[-3]))
        os.makedirs(output_folder, exist_ok=True)
        output_fp = os.path.join(output_folder, split_filename[-1])
        print(output_fp)
        return output_fp

    def save_file(self, filename):
        output_fp = self.generate_output_fp(filename)
        self.cph_ds.to_netcdf(output_fp)


class OutputFilteredFile(OutputFile):
    def generate_output_fp(self, filename, min_temp, max_temp):
        split_filename = filename.split(os.path.sep)
        split_filename[-1] = split_filename[-1][0:11] + \
            f"T_{abs(min_temp)}_{abs(max_temp)}_" + \
            split_filename[-1][11:]
        output_folder = os.path.join(
            CLAAS_FP, "Filtered_data", *(split_filename[-3:-1]))
        os.makedirs(output_folder, exist_ok=True)
        output_fp = os.path.join(output_folder, split_filename[-1])
        print(output_fp)
        return output_fp

    def save_file(self, filename, min_temp, max_temp):
        output_fp = self.generate_output_fp(filename, min_temp, max_temp)
        self.cph_ds.to_netcdf(output_fp)


def generate_output(filenames_it):
    transformer = ProjectionTransformer()
    aux_data = xr.load_dataset(os.path.join(
        CLAAS_FP, 'claas3_level2_aux_data.nc'), decode_times=False)
    transformer.generate_lat_lon_prj(aux_data)
    for filename in filenames_it:
        # xr.load_mfdataset(filenames_it)

        # Load new dataset
        ctx_filename_list = filename.split(os.path.sep)
        ctx_filename_list[-1] = ctx_filename_list[-1].replace("CPPin", "CTXin")
        ctx_filename_list[-2] = ctx_filename_list[-2].replace("CPH", "CTT")
        
        # Open relevant datasets
        input_cpp_ds = xr.load_dataset(filename)
        input_ctx_ds = xr.load_dataset(
            os.path.join(os.path.sep, *(ctx_filename_list)))

        # Resample dataset contents
        resampled_cph_data = transformer.remap_data(input_cpp_ds["cph"].data)
        resampled_ctt_data = transformer.remap_data(input_ctx_ds["ctt"].data)
        resampled_cth_data = transformer.remap_data(input_ctx_ds["cth"].data)
        print("Resampled: ", filename)
        # Generate output file and save result
        output_file = OutputResampledFile(input_cpp_ds, input_ctx_ds, 1)
        output_file.add_coords(transformer.new_cord_lat,
                               transformer.new_cord_lon)
        output_file.set_output_variables(
            resampled_cph_data, resampled_ctt_data, resampled_cth_data)
        output_file.save_file(filename)
        # Close dataset
        input_cpp_ds.close()


if __name__ == "__main__":
    print("Generating resample target filenames")
    resample_target_filenames = filenames_to_resample(start_time=datetime(
        2004, 1, 20, 4, 5), end_time=datetime(2004, 1, 20, 5, 5))
    print(resample_target_filenames)
    print("Resample target filenames generated")
    print("Resampling needed files")
    generate_output(resample_target_filenames)
    print("Needed files resampled")
