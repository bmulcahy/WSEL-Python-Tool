# WSEL Python Tool
## First iteration of the WSEL script.
### Created by Brian Mulcahy
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

This script will iterate over the WSEL fields specified in the 'runs' variable under the main function

The root folder is assigned to the rootdir variable. Change the rootdir variable to the directory with the above folder and file structure

Change the sr variable to match the desired spatial reference the rasters should be in

Change the projectname to match the name of the overall watershed

Change the main_stream to match the name of the main stream of the watershed, all streams should flow into this stream


This script requires both the spatial and 3D analyst licenses.


###### TODO:
- create config process
- Add option for single stream direct plug in to the script skipping the filename/structure configuration
- expand on multiprocessor options
- [x] add toggle for backwater adjustments
- Catch arcgis specific exceptions
- Make the script more intuitive in searching for files and folders to be more flexible in file and folder naming
