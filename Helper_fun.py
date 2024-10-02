import numpy as np
#####
#find time indexes at which there is data from both datasets
#returns (indexes of common time in time1 , ---//--- in time2)
#####
def time_intersection(time1,time2):
    return (np.nonzero(np.in1d(time1, time2))[0] , np.nonzero(np.in1d(time2, time1))[0])