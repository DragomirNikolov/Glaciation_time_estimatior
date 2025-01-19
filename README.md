# Glaciation Time Estimator

**Glaciation Time Estimator (GTE)** is a climate data analysis tool used to measure cloud glaciation time (CGT) in stratiform clouds using SEVIRI satellite data. The process is divided into the following steps:

1. **Preprocessing**  
   - Resampling is performed to convert geostationary data to a regular latitude/longitude reference frame.  
   - Cloud top temperature filtering is applied to refine the data.

2. **Tracking**  
   - The PyFLEXTRKR library is utilized to track filtered cloud fragments for further analysis.

3. **Postprocessing**  
   - The tracked fragments are analyzed, and data for each cloud is stored in a Parquet binary file format.

4. **Result Analysis**  
   - Cloud glaciation is identified, and the CGT is measured.
