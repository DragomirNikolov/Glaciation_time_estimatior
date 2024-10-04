import numpy as np
#####
#find time indexes at which there is data from both datasets
#returns (indexes of common time in time1 , ---//--- in time2)
#####
def time_intersection(time1:np.array,time2:np.array)->tuple:
    return (np.nonzero(np.in1d(time1, time2))[0] , np.nonzero(np.in1d(time2, time1))[0])

#Return a 2d array of [[min_temp][max_temp]].T.  Use with: for t_max,t_min in t_ranges:
def generate_temp_range(t_deltas:list)->np.array:
    for dt in t_deltas:
        boundary_temps=np.arange(0,-38.1,-dt)
        t_ranges_temp=np.concatenate((boundary_temps[1:][None,:].astype(int),boundary_temps[0:-1][None,:].astype(int)),0,dtype='int')
        try:
            t_ranges=np.concatenate((t_ranges,t_ranges_temp),1,dtype='int')     
        except:
            t_ranges=t_ranges_temp
    return t_ranges.T