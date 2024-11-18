from numba.typed import Dict, List #, UniTuple
from numba.core import types

#key_type=types.unicode_type, value_type=types.ListType(types.tuple)
@jit
def analyze_field(cloudtracknumber_field):
    # Define the dictionary with the appropriate types
    # loc_hash_map_cloud_numbers = Dict.empty(
    #     key_type=types.int64,  # Keys are integers
    #     value_type=types.ListType(types.UniTuple(types.int64, 2))  # Values are lists of 2-tuples
    # )
    loc_hash_map_cloud_numbers = Dict.empty(
        key_type=types.int64,  # Keys are integers
        value_type=types.ListType(types.int64) #types.UniTuple(types.int64, 2))   Values are lists of 2-tuples
    )
    
    # # Traverse the 3D array
    # for row in range(cloudtracknumber_field.shape[1]):
    #     for col in range(cloudtracknumber_field.shape[2]):
    #         val = int(cloudtracknumber_field[0, row, col])
    val=1
            
            # If the key doesn't exist, initialize an empty list
            # if val not in loc_hash_map_cloud_numbers:
            #     loc_hash_map_cloud_numbers[val] = List.empty_list(types.UniTuple(types.int64, 2))
            
            # Append the tuple to the list
            # location = (int(row), int(col))  # Explicitly cast to int64
            # loc_hash_map_cloud_numbers[val].append(location)
    return None
    # return loc_hash_map_cloud_numbers


test_array = np.array([[[1, 2],[3 , 3]]])
hash_map_cloud_numbers=analyze_field(test_array)