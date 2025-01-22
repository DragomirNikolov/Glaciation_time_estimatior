# Glaciation Time Estimator

**Glaciation Time Estimator (GTE)** is a climate data analysis tool used to measure cloud glaciation time (CGT) in stratiform clouds. It analyses the CLAAS-3 dataset based on Meteosat Second Generaion satellite data. The processing is divided into the following steps:

1. **Preprocessing**  
   - Resampling is performed to convert geostationary data to a regular latitude/longitude reference frame.  
   - Cloud top temperature filtering is applied to split larger clouds into individual temperature homogenious cloud segments
   - The data is aggregated in n x n bins (usually n=3) for reduced processing time and data storage

2. **Tracking**  
   - The PyFLEXTRKR library is utilized to track the resulting cloud segments. The library tracks segments with area larger than 1000 [km^2] surviving more than 45 min 

3. **Postprocessing**  
   - The tracked fragments are analyzed, and data for each cloud is stored in a Parquet binary file format. 

4. **Result Analysis**  
   - Cloud glaciation is identified, and the CGT is measured.
