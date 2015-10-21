# WSEL Python Tool
## First iteration of the WSEL script.

This script relies on folder and file structure as follows:
- Watershed(folder)
  -  Stream
    -  stream.shp(streamline shape file)
    -  stream_xsecs_results.shp(cross section shape file with wsel noted in the table as WSE)
    -  stream_flood.shp(flood boundary shape file)
  -  Stream2
    -  stream2.shp(streamline shape file)
    -  stream2_xsecs_results.shp(cross section shape file with wsel noted in the table as WSE)
    -  stream2_flood.shp(flood boundary shape file)


  etc....


The root folder is assigned to the rootdir variable. Change the rootdir variable to the directory with the above folder and file structure
Change the sr variable to match the desired spatial reference the rasters should be in
Change the projectname to match the name of the overall watershed

This script requires both the spatial and 3D analyst licenses.


###### TODO:
- create config process
- expand on multiprocessor options
- add toggle for backwater adjustments
- Catch arcgis specific exceptions
- Make the script more intuitive in searching for files and folders to be more flexible in file and folder naming
