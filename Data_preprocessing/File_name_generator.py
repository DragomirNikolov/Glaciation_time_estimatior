from datetime import datetime, timedelta
import numpy as np
import os
from pandas import date_range
import numpy.core.defchararray as np_f
# from ..Helper_fun import generate_temp_range


def round_time(tm, round_increment):
    """Rounds down the datetime `tm` down to the nearest `round_increment` in minutes."""
    return tm - timedelta(minutes=tm.minute % round_increment,
                          seconds=tm.second,
                          microseconds=tm.microsecond)


def intertwine_iterable(*lists):
    tot_lenth = 0
    for list in lists:
        tot_lenth += len(list)
    intertwined_list = [None]*tot_lenth
    for list_ind in range(len(lists)):
        intertwined_list[list_ind::len(lists)] = lists[list_ind]
    return intertwined_list


class MissingFilesSearcher:
    def __init__(self, start_time, end_time, t_deltas):
        round_increment = 15
        self.start_time = round_time(start_time, round_increment)
        self.end_time = round_time(end_time, round_increment)

        self.t_deltas = t_deltas
        self.temps_to_check = self.gen_temps_to_check()
        # CLAAS_FP shouldnt contain the CPP

        self.CLAAS_FP = os.environ["CLAAS_DIR"]
        # print(CLAAS_FP)
        self.pole_split = True
        self.pole_folders = ['np', 'sp']
        self.agg_fact = 2
        self.t_deltas = [3, 5]

    def gen_filename_list(self, file_format="%Y/%m/%d/CPPin%Y%m%d%H%M%S.nc", freq=timedelta(minutes=15)):
        filenames = [self.start_time.strftime(file_format)]
        filenames.extend(date_range(self.start_time, self.end_time,
                                    freq=freq).strftime(file_format).to_list())
        return filenames

    def gen_temps_to_check(self):
        temps_to_check_max = [0 for dt in self.t_deltas]
        temps_to_check_min = [abs(0-dt) for dt in self.t_deltas]
        return temps_to_check_max, temps_to_check_min

    def gen_filtered_filenames(self, CLAAS_FP_POLE):
        filtered_first_dt_filenames = []
        for temp_ind in range(len(self.temps_to_check[0])):
            min_temp = self.temps_to_check[0][temp_ind]
            max_temp = self.temps_to_check[1][temp_ind]
            filtered_fp_format = os.path.join(
                CLAAS_FP_POLE, f"%Y/%m/%d/Agg_{self.agg_fact:02}_T_{max_temp:02}_{min_temp:02}_%Y%m%d%H%M%S.nc")
            filtered_first_dt_filenames.append(
                np.array(self.gen_filename_list(file_format=filtered_fp_format)))
        filtered_first_dt_filenames = np.array(
            intertwine_iterable(*(filtered_first_dt_filenames)))
        return filtered_first_dt_filenames

    def are_missing(self, filenames):
        vec_isfile = np.vectorize(os.path.isfile)
        file_exists_bool = vec_isfile(filenames)
        return np.invert(file_exists_bool)

    def gen_missing_filtered_filenames(self, CLAAS_FP_POLE):
        filtered_first_dt_filenames = self.gen_filtered_filenames(
            CLAAS_FP_POLE)
        self.filtered_file_missing_bool = self.are_missing(
            filtered_first_dt_filenames)

        self.missing_filtered_filenames = filtered_first_dt_filenames[
            self.filtered_file_missing_bool]
        # return self.missing_filtered_filenames

    def gen_resample_ind(self, file_missing_bool):
        n_temps = len(self.temps_to_check[0])
        resample_ind = np.zeros(
            int(len(file_missing_bool)/n_temps), dtype=bool)
        for temp_ind in range(n_temps):
            resample_ind = resample_ind | file_missing_bool[temp_ind::n_temps]
        return resample_ind

    def gen_filenames_to_filter(self, CLAAS_FP_POLE):
        resampled_fp_format = os.path.join(
            CLAAS_FP_POLE, f"%Y/%m/%d/Agg_{self.agg_fact:02}_%Y%m%d%H%M%S.nc")
        resample_filenames = np.array(
            self.gen_filename_list(file_format=resampled_fp_format))
        resample_ind = self.gen_resample_ind(self.filtered_file_missing_bool)
        self.filenames_to_filter = resample_filenames[resample_ind]

    def gen_filenames_to_resample(self, CLAAS_FP_POLE):
        cpp_fp_format = os.path.join(
            CLAAS_FP_POLE, "%Y/%m/%d/CPPin%Y%m%d%H%M%S405SVMSG01MD.nc")
        ctx_fp_format = cpp_fp_format.replace("CPP", "CTX")
        ind_to_resample = self.are_missing(self.filenames_to_filter)
        # print(ind_to_resample)
        self.cpp_files_to_resample = np.array(self.gen_filename_list(
            file_format=cpp_fp_format))[ind_to_resample]
        self.ctx_files_to_resample = np.array(self.gen_filename_list(
            file_format=ctx_fp_format))[ind_to_resample]

    def gen_target_filenames(self):
        if self.pole_split:
            self.result_dict = {pole: {"filter": None, "resample_CPP": None,
                                       "resample_CTX": None} for pole in self.pole_folders}
            for pole in self.pole_folders:
                CLAAS_FP_POLE = os.path.join(self.CLAAS_FP, pole)
                self.gen_missing_filtered_filenames(CLAAS_FP_POLE)
                self.gen_filenames_to_filter(CLAAS_FP_POLE)
                self.gen_filenames_to_resample(CLAAS_FP_POLE)
                self.result_dict[pole]["filter"] = self.filenames_to_filter
                self.result_dict[pole]["resample_CPP"] = self.cpp_files_to_resample
                self.result_dict[pole]["resample_CTX"] = self.ctx_files_to_resample
        else:
            self.result_dict = {"filter": None,
                                "resample_CPP": None, "resample_CTX": None}
            self.gen_missing_filtered_filenames(self.CLAAS_FP)
            self.gen_filenames_to_filter(self.CLAAS_FP)
            self.gen_filenames_to_resample(self.CLAAS_FP)
            self.result_dict["filter"] = self.filenames_to_filter
            self.result_dict["resample_CPP"] = self.cpp_files_to_resample
            self.result_dict["resample_CTX"] = self.ctx_files_to_resample
            # raise NotImplementedError("Not implemented for non split poles")

print("Start")
start_time = datetime(2004, 1, 20, 4, 5)
end_time = datetime(2024, 1, 21, 5, 5)
t_deltas = [3, 5]
searcher = MissingFilesSearcher(start_time, end_time, t_deltas)
searcher.gen_target_filenames()
# print(searcher.result_dict)
print("Dome")


# def generate_filenames(start_time, end_time, file_format="%Y/%m/%d/CPPin%Y%m%d%H%M%S.nc",  target_months=None, round_increment=15):
#     """
#     Generates a list of folder paths based on time intervals.

#     Parameters:
#         start_time (datetime): Start time.
#         end_time (datetime): End time.
#         round_increment (int): Time interval in minutes.

#     Returns:
#         list: List of folder paths for each time interval.
#     """
#     # Round start and end times
#     start_time = round_time(start_time, round_increment)
#     end_time = round_time(end_time, round_increment)
#     if target_months is not None:
#         raise NotImplementedError(
#             "You need to add the case where the function is searching only within certain months")

#         # Generate array of time incremented times
#         filenames = []
#         for month in target_months:
#             if month == start_time.strftime(folder_format):
#                 month_end = last_day_of_month(month)
#                 if end_time < month_end:
#                     filenames.extend(gen_filename_list(start_time, end_time))
#                 else:
#                     filenames.extend(gen_filename_list(start_time, month_end))
#             elif month == end_time.strftime(folder_format):
#                 month_start = first_day_of_month(month)
#                 filenames.extend(gen_filename_list(month_start, end_time))
#             else:
#                 month_start = first_day_of_month(month)
#                 month_end = last_day_of_month(month)
#                 filenames.extend(gen_filename_list(month_start, month_end))
#     else:
#         filenames = gen_filename_list(start_time, end_time, file_format)
#     # Generate folder paths

#     return filenames
