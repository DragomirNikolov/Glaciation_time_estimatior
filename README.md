#Glactiation time estimator
Glaciation_time_estimator (GTE) is a climate data analysis tool used to measure cloud glaciation time (CGT) in stratiform clouds from SEVIRI satellite data. The process is divided into 3 main steps:
-Preprocessing: Resampling from geostationary to regular lat/lon reference frame and Cloud top temperature filtering
-Tracking: The PyFLEXTRKR library is used to track the iltered cloud fragments
-Postprocessing: The tracked fragmetns are analysed and data on each cloud is stored into a parqut binary
-Result Analysis: Cloud glaciation is identified and CGT is measured
