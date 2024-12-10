import os
import xarray as xr
import numpy as np

global CLAAS_FP
CLAAS_FP = os.environ["CLAAS_DIR"]


class OutputFile:
    def __init__(self, cph_dataset, agg_fact):
        self.cph_ds = cph_dataset
        self.agg_fact = agg_fact

    def add_coords(self, lats, lons):
        self.cph_ds = self.cph_ds.assign_coords({"lon": lons, "lat": lats})
        self.cph_ds.lat.attrs = {"standard_name": "latitude",
                                 "long_name": "latitude",
                                 "units": "degrees_north", }
        self.cph_ds.lon.attrs = {"standard_name": "longitude",
                                 "long_name": "longitude",
                                 "units": "degrees_east", }

    def set_cpp_output_variables(self, resampled_cph_data):
        self.cph_ds["cph_resampled"] = xr.DataArray(
            resampled_cph_data,
            dims=("time", "lat", "lon"),
            attrs={
                "cell_methods": "time: point",
                "flag_meanings": "clear liquid ice",
                "flag_values": "0s, 1s, 2s",
                "missing_value": np.int16(-1),
                # "grid_mapping": "projection",
                'coordinates': 'lon lat',
                "units": "1",
                "long_name": "Cloud Thermodynamic Phase",
                "standard_name": "thermodynamic_phase_of_cloud_water_particles_at_cloud_top",
                "_FillValue": np.int16(-1),
            }
        )
    def set_ctx_output_variables(self, resampled_ctt_data, resampled_cth_data):
        self.cph_ds["ctt_resampled"] = xr.DataArray(
            resampled_ctt_data.astype(np.float32),
            dims=("time", "lat", "lon"),
            attrs={
                "_FillValue": np.float32(-1),
                "units": "K",
                "valid_range": [np.float32(0), np.float32(4060)],
                "standard_name": "air_temperature_at_cloud_top",
                "long_name": "Cloud Top Temperature",
                # "grid_mapping": "projection",
                'coordinates': 'lon lat',
                "cell_methods": "time: point",
                "add_offset": np.float32(0.0),
                # "scale_factor": np.float32(0.1)
            }
        )
        self.cph_ds["cth_resampled"] = xr.DataArray(
            resampled_cth_data.astype(np.float32),
            dims=("time", "lat", "lon"),
            attrs={
                "_FillValue": np.float32(-1),
                "units": "m",
                "valid_range": [np.float32(0), np.float32(30000)],
                "standard_name": "cloud_top_altitude",
                "long_name": "Cloud Top Height",
                # "grid_mapping": "projection",
                'coordinates': 'lon lat',
                "cell_methods": "time: point",
                "add_offset": np.float32(0.0),
                "scale_factor": np.float32(1.0)
            }
        )


class OutputResampledFile(OutputFile):

    def save_file(self, output_fps):
        os.makedirs(os.path.dirname(output_fps[0]), exist_ok=True)
        _, dataset_list = zip(*(self.cph_ds.groupby("time")))
        xr.save_mfdataset(dataset_list, list(output_fps))


class OutputFilteredFile(OutputFile):

    def generate_output_fp(self, fp, min_temp, max_temp):
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        reduced_fp = fp.strip(os.path.join(CLAAS_FP, "Resampled_data"))
        output_fp = reduced_fp.replace(
            f"Agg_{self.agg_fact:02}_", f"Agg_{self.agg_fact:02}_T_{abs(min_temp):02}_{abs(max_temp):02}_")
        output_fp = os.path.join(CLAAS_FP, "Filtered_Data", output_fp)
        os.makedirs(os.path.dirname(output_fp), exist_ok=True)
        return output_fp

    def save_file(self, filename, min_temp, max_temp):
        output_fp = self.generate_output_fp(filename, min_temp, max_temp)
        self.cph_ds.to_netcdf(output_fp)
