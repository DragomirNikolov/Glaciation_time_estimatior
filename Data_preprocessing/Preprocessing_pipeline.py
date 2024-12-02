import os
import xarray as xr
from Resample_data import ProjectionTransformer
from Resampling_file_search import filenames_to_resample
from datetime import datetime

global CLAAS_FP
CLAAS_FP = "/cluster/work/climate/dnikolo/CLAAS_Data/"


class OutputFile:
    def __init__(self, dataset):
        self.ds = dataset

    def add_coords(lats, lons):
        self.ds.assign_coords({"lon": lons, "lat": lats})

    def add_output_variable(resampled_data):
        ds["cph_resampled"] = xr.DataArray(
            cph_data,
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

    def save_file(output_fp):
        self.ds.to_netcdf(output_fp)

def generate_output(filenames_it):
    transformer = ProjectionTransformer()
    aux_data = xr.load_dataset(os.path.join(
        CLAAS_FP, 'claas3_level2_aux_data.nc'), decode_times=False)
    transformer.generate_lat_lon_prj(aux_data)
    for filename in filenames_it:
        # xr.load_mfdataset(filenames_it)
        # Load new dataset
        input_ds = xr.load_dataset(filename)
        # Resample dataset contents
        resampled_data = transformer.remap_data(data["cph"].data)
        output_fp = os.path.join(
            CLAAS_FP, "Resampled_data", *(filename.split(os.path.sep)[-3:]))
        print(output_fp)
        # Save file
        output_file = OutputFile(output_fp, input_ds)
        output_file.add_coords()
        output_file.add_output_variable(resampled_data)
        output_file.save_file(output_fp)
        # Close dataset

        input_ds.close()


if __name__ == "__main__":
    print("Generating resample target filenames")
    resample_target_filenames = filenames_to_resample(start_time=datetime(
        2004, 1, 20, 12, 5), end_time=datetime(2004, 1, 20, 13, 5))
    print(resample_target_filenames)
    print("Resample target filenames generated")
    print("Resampling needed files")
    generate_output(resample_target_filenames)
    print("Needed files resampled")
