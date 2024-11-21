from numba.typed import Dict, List #, UniTuple
from numba.core import types
import numba as nb
import numpy as np
#key_type=types.unicode_type, value_type=types.ListType(types.tuple)



@nb.jit
def analyze_field(cloudtracknumber_field, max_size):
    # Define the dictionary with the appropriate types
    loc_hash_map_cloud_numbers={1: (0,np.zeros((2,max_size),dtype=np.int16))}
    # # Traverse the 3D array
    for row in range(cloudtracknumber_field.shape[1]):
        for col in range(cloudtracknumber_field.shape[2]):
            val = cloudtracknumber_field[0, row, col]
            if val not in loc_hash_map_cloud_numbers:
                loc_hash_map_cloud_numbers[val] = (0,np.empty((2,max_size),dtype=np.int16))
            ind,cord=loc_hash_map_cloud_numbers[val]
            if ind<=max_size:
                cord[:,ind]=np.asarray([row,col],dtype=np.int16)
                ind+=1
                print(ind)
                loc_hash_map_cloud_numbers[val] = (ind,cord)
    return loc_hash_map_cloud_numbers
    # return loc_hash_map_cloud_numbers


test_array = np.array([[[1, 2],[3 , 3]]])
print("We are getting somewhere")
hash_map_cloud_numbers=analyze_field(test_array,20)
# for val in hash_map_cloud_numbers:
#         ind ,cord=hash_map_cloud_numbers[val]
#         cord_1=cord[:,:ind]
#         hash_map_cloud_numbers[val] = (ind,cord_1)
print("It runs")
# Print result
for key in hash_map_cloud_numbers:
    print(f"Value: {key}, Locations: {list(hash_map_cloud_numbers[key])}")