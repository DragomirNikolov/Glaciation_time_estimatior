import numpy as np
import xarray as xr

class Cloud:
    # def __new__(self, *args, **kwargs):
    #     return super().__new__(self)
    def __init__(self,cloud_id):
        self.id=cloud_id
        self.crit_fraction=0.1
        # Bools inidicating if the cloud has been liquid at any point
        self.is_liq: bool =False
        self.is_mix: bool =False
        self.is_ice: bool =False
        # Max and min cloud size in pixels
        self.max_size_km: float =0.0
        self.max_size_px: int = 0
        self.min_size_km: float =510.0e6
        self.min_size_px: int = 3717*3717
        
        #Variables giving the first and last 4 timesteps (1 hour) of the cloud ice fraction - both arrays run in the same time direction start: [1 , 2 , 3 , 4] ... end: [1 , 2 , 3 , 4]
        self.start_ice_fraction_arr=np.empty(4)
        self.end_ice_fraction_arr=np.empty(4)
        # self.ice_fraction_arr=np.empty(max_timesteps)
        self.ice_fraction_list=[]

        self.max_water_fraction:float=0.0
        self.max_ice_fraction:float=0.0

        self.track_start_time: dt.datetime=None
        self.track_end_time: dt.datetime=None
        self.track_length = None

        self.glaciation_start_time: dt.datetime=None
        self.glaciation_end_time: dt.datetime=None

        self.n_timesteps=None

        self.sum_cloud_lat=0.0
        self.sum_cloud_lon=0.0
        self.avg_cloud_lat=None
        self.avg_cloud_lon=None

        self.sum_cloud_size_km=0.0
        self.avg_cloud_size_km=None

        self.sum_cloud_size_px=0.0
        self.avg_cloud_size_px=None

        self.n_timesteps_no_cloud=0
        self.terminate_cloud=False

        
    def __str__(self):
        return f"{self.is_liq},{self.is_mix},{self.is_ice},"

    def update_status(self,time: dt.datetime, cloud_values: np.array,cloud_lat,cloud_lon,lat_resolution,lon_resolution):
        cloud_size_px=cloud_values.shape[0]
        if cloud_size_px:
            self.n_timesteps_no_cloud=0
            water_fraction=float(np.count_nonzero(cloud_values==1))/float(cloud_size_px)
            # ice_fraction=float(np.count_nonzero(cloud_values==2))/float(cloud_size_px)
            ice_fraction=1-water_fraction
            # assert math.isclose(water_fraction+ice_fraction,1)
            #print(water_fraction)
            #print(water_fraction)
            if not (self.track_start_time):
                self.track_start_time=time
                self.n_timesteps=1
            else:
                self.n_timesteps+=1
            if self.n_timesteps<=4:
                self.start_ice_fraction_arr[self.n_timesteps-1]=ice_fraction
            #Check and set type of cloud
            if water_fraction>1-self.crit_fraction:
                self.is_liq=True
            elif water_fraction>self.crit_fraction:
                self.is_mix=True
            else:
                self.is_ice=True

            cloud_size_km=lat_resolution*lon_resolution*cloud_size_px*np.cos(np.deg2rad(cloud_lat))*111.321*111.111

            self.max_size_km=max(self.max_size_km, cloud_size_km)
            self.min_size_km=min(self.min_size_km, cloud_size_km)

            self.max_size_px=max(self.max_size_px, cloud_size_px)
            self.min_size_px=min(self.min_size_px, cloud_size_px)

            self.sum_cloud_size_px+=cloud_size_px
            self.avg_cloud_size_px=self.sum_cloud_size_px/self.n_timesteps
            
            self.sum_cloud_size_km+=cloud_size_km
            self.avg_cloud_size_km=self.sum_cloud_size_km/self.n_timesteps
            

            # I assume that water_frac+ice_frac=1
            
            self.max_water_fraction=max(self.max_water_fraction, water_fraction)
            self.max_ice_fraction=max(self.max_ice_fraction, 1-water_fraction)

            self.sum_cloud_lat+=cloud_lat
            self.sum_cloud_lon+=cloud_lon
            self.avg_cloud_lat=self.sum_cloud_lat/self.n_timesteps
            self.avg_cloud_lon=self.sum_cloud_lon/self.n_timesteps

            self.track_end_time=time
            self.track_length=self.track_end_time-self.track_start_time
            
            self.end_ice_fraction_arr[0:3]=self.end_ice_fraction_arr[1:4]
            self.end_ice_fraction_arr[3]=ice_fraction
            
            # self.ice_fraction_arr[n_timesteps]=ice_fraction
            self.ice_fraction_list.append(ice_fraction)
            
            
    def update_missing_cloud(self):
        if self.track_end_time and (not self.terminate_cloud):
            self.n_timesteps_no_cloud+=1
            if self.n_timesteps_no_cloud > 1:
                self.terminate_cloud=True