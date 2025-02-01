import threading
import subprocess
import os 
os.environ['OPENBLAS_NUM_THREADS'] = '1'
import numpy as np


def dispatch(sem, argv, **kw):
    try:
        for args in argv[:-1]:
            subprocess.run(args, **kw)
        for file in argv[-1]:
            os.remove(file)
    finally:
        sem.release()
        return

def generate_filtered_output_fps(
                day_fp_to_filter, agg_fact, min_temp, max_temp):
    return np.char.replace(
        day_fp_to_filter, f"Agg_{agg_fact:02}", f"output_T_{abs(min_temp):02}_{abs(max_temp):02}")
def filtering_worker_cdo(day_fp_to_filter, temp_bounds, agg_fact):
    sem = threading.Semaphore(4)   # pick a threshold here
    for temp_ind in range(len(temp_bounds[0])):
        min_temp = temp_bounds[0][temp_ind]
        max_temp = temp_bounds[1][temp_ind]
        outpur_fps = generate_filtered_output_fps(
                day_fp_to_filter, agg_fact, min_temp, max_temp)
        Ts = []
        for file_ind, output_fp in enumerate(outpur_fps):
            sem.acquire()
            argv=[["cdo","-setmisstoc,0", f'-expr,cph_filtered = cph_resampled*(ctt_resampled<{(273.15+max_temp):0.2f} && ctt_resampled>{(273.15+min_temp):0.2f})', day_fp_to_filter[file_ind], output_fp],
                   []
                  ]
            T = threading.Thread(target=dispatch, args=(sem, argv))
            T.start()
            Ts.append(T)
        for T in Ts:
            T.join()
    return

day_fp_to_filter = np.array(["/wolke_scratch/dnikolo/CLAAS_Data/Resampled_Data/sp/2022/01/01/Agg_03_20220101231500.nc",
          "/wolke_scratch/dnikolo/CLAAS_Data/Resampled_Data/sp/2022/01/01/Agg_03_20220101233000.nc",
            "/wolke_scratch/dnikolo/CLAAS_Data/Resampled_Data/sp/2022/01/01/Agg_03_20220101234500.nc"])
filtering_worker_cdo(day_fp_to_filter,[[-5,-10,-15],[0,-5,  -10]],3 )
