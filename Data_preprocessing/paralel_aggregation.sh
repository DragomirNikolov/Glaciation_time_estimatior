#!/bin/bash
. `which env_parallel.bash`
env_parallel --session ;
env_parallel --jobs 4 < $1
