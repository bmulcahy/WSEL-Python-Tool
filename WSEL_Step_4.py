from __future__ import print_function
import sys, os, re, arcpy, traceback
from arcpy import env
from arcpy.sa import *
from safe_print import Safe_Print
### Created by Brian Mulcahy ##
# Step 4 creates a polyline-zm from the given streamlines and cross-sections
# It first creates a polyline-zm where the m value is the WSEL
# it then moves the m value to the z value by doing a 3D by attribute process on the point vertices
# the pt vertices are then turned back into a line and spatially joined back with the original stream line
# to perserve its' original attributes. Now with the newly create polyline-m stream-line when then use this as
# input into the route creation function the same as before but this time using the ProfileM/Section as the value.
# In order to insure that interpolation perfers correctly and that it does not interpolate below 0 on the streamline
# we manually create a zero station that is appended to the beginning of the stream vertices pt file
# 
class WSEL_Step_4:

    def __init__(self, config, streams):
        self.streams = streams
        self.config = config
        arcpy.CheckOutExtension("3D")

    def __enter__(self):
        self.scratch = self.config['scratch']
        self.table_folder =self.config['table_folder']
        self.scratchgdb = self.config['scratchgdb']
        self.xs_original = self.config['xs_original']
        self.output_workspace = self.config['output_workspace']
        self.xs_dataset = self.config['xs_dataset']
        self.streams_original = self.config['streams_original']
        self.flood_original =self.config['flood_original']
        self.xs_intersect_dataset = self.config['xs_intersect_dataset']
        self.streams_intersect_dataset = self.config['streams_intersect_dataset']
        self.routes_dataset = self.config['routes_dataset']
        self.streams_dataset = self.config['streams_dataset']
        self.vertices_dataset = self.config['vertices_dataset']
        self.streams_zm =self.config['streams_zm']
        self.backwater =self.config['backwater']
        self.sr = self.config['sr']
        self.multi=self.config['multiproc']
        self.modelbuilder=self.config['modelbuilder']
        self.print_config = {'multi': self.multi, 'modelbuilder': self.modelbuilder}
        self.safe_print = Safe_Print(self.print_config)
        env.workspace = self.scratch
        #env.parallelProcessingFactor = "4"
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return

    #Gets intersection between stream's centerline and the stream's xs
    def get_intersection(self, stream, xs, name):
        self.safe_print.print_out("Getting Intersection between "+name+"'s Stream and XS files")
        inFeatures = [stream, xs]
        intersectOutput = self.xs_intersect_dataset+"/"+name+"_xs_pt"
        clusterTolerance = 0
        pt = arcpy.Intersect_analysis(inFeatures, intersectOutput, "ALL", clusterTolerance, "POINT")
        self.feature = arcpy.FeatureToPoint_management(pt, self.xs_intersect_dataset+"/"+name+"_xs_pt_feature","CENTROID")
        arcpy.Delete_management(pt)
        return self.feature

    #Creates zm attributes on the stream centerline using linear referencing tools
    def add_routes(self,stream,xs_pt,name,status):

        rid = "Route_ID"

        if status == 0:
            self.safe_print.print_out("Adding Z-values to Polyline ZM")
            mfield="WSEL_REG"
            stationList = [r[0] for r in arcpy.da.SearchCursor (xs_pt, ["XS_Station"])]#gives me a list of all the stations/profilem
            min_station = min(stationList)# gives me the minimum xs value(should be 0.01)
            if min_station > 0:
                self.safe_print.print_out("Creating zero station before processing")
                pts = self.add_zero_station(stream, xs_pt,min_station,name)
        if status == 1:
            self.safe_print.print_out("Adding M-values to Polyline ZM")
            mfield ="XS_Station"
            #need to add 0 station at beginning of line if there is none, other wise the tool may encounter a boundary error with
            #a m-value being less than 0
            stationList = [r[0] for r in arcpy.da.SearchCursor (xs_pt, [mfield])]#gives me a list of all the stations/profilem
            min_station = min(stationList)# gives me the minimum station value(should be 0.01)
            if min_station > 0:
                self.safe_print.print_out("Creating zero station before processing")
                pts = self.add_zero_station(stream, xs_pt,min_station,name)
            else:
                pts = xs_pt

        out_fc = self.routes_dataset+"/"+name+"_stream_routes" 
        rts = stream
        out_routes = self.routes_dataset+"\\stream_measures"
        route_evt_layer_temp =name+"_evt_lyr"
        route_evt_layer= self.scratch+"\\"+name+"_evt"
        props = "RID POINT MEAS"
        out_table =self.table_folder+"\\route_loc.dbf"

        route_meas = arcpy.CreateRoutes_lr(rts, rid, out_routes,"LENGTH", "#", "#", "UPPER_LEFT",1,0,"IGNORE", "INDEX")#creates an m field based on the length of the streamline
        loc_features = arcpy.LocateFeaturesAlongRoutes_lr(pts, route_meas, rid, "0", out_table, props, 'FIRST', 'NO_DISTANCE','NO_ZERO','FIELDS')#finds the points along the streamline based of the m-field found in previous process
        evt_lyr = arcpy.MakeRouteEventLayer_lr(route_meas, rid, loc_features, props, route_evt_layer_temp, "#",  "NO_ERROR_FIELD",  "NO_ANGLE_FIELD","NORMAL","ANGLE", "LEFT", "POINT")#creates a layer based of the route table from previous process
        lyr = arcpy.SaveToLayerFile_management(evt_lyr, route_evt_layer, "RELATIVE")#may or may not need to save layer to disc. I only did this when I was trying to use parrallel processing
        #depending on which time the process is called either the WSEL or Station field is used
        if status == 0:
            routes = arcpy.CalibrateRoutes_lr (route_meas, rid, lyr, rid, mfield, out_fc,"MEASURES","0","BETWEEN","BEFORE","AFTER","IGNORE","KEEP","INDEX")
        else:
            routes = arcpy.CalibrateRoutes_lr (route_meas, rid, lyr, rid, mfield, out_fc,"MEASURES","0","BETWEEN","NO_BEFORE","AFTER","IGNORE","KEEP","INDEX")
        arcpy.Delete_management(loc_features)
        arcpy.Delete_management(route_meas)
        arcpy.Delete_management(lyr)
        return routes



    def add_zero_station(self, stream, xs_pt, min_station, name):
        tempLayer="min_stationxs"
        temp_pt_Layer="zero_station"
        keep_fields = [f.name for f in arcpy.ListFields(xs_pt)]
        fieldName = "XS_Station"
        fieldName2 = "OBJECTID"
        sqlExp = "{0} = {1}".format(fieldName, min_station)
        sqlExp2 = "'{0}'".format(name)
        arcpy.MakeFeatureLayer_management(xs_pt, tempLayer)
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION", sqlExp)
        pts = arcpy.FeatureVerticesToPoints_management(stream, self.vertices_dataset+'/'+name+"_endpts","BOTH_ENDS")
        stream_startend= arcpy.FeatureToPoint_management(pts,self.vertices_dataset+'/'+ name+"_endpts_feature","CENTROID")
        arcpy.Delete_management(pts)
        arcpy.Near_analysis(tempLayer, stream_startend)
        start_oid =[r for r in arcpy.da.SearchCursor (xs_pt,("WSEL_REG","NEAR_FID"),where_clause=sqlExp)][0]        
        sqlExp3 = "{0} <> {1}".format(fieldName2, start_oid[1])
        arcpy.MakeFeatureLayer_management(stream_startend, temp_pt_Layer)
        arcpy.SelectLayerByAttribute_management(temp_pt_Layer, "NEW_SELECTION", sqlExp3)
        if int(arcpy.GetCount_management(temp_pt_Layer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(temp_pt_Layer)
        arcpy.AddField_management(stream_startend,'XS_Station',"FLOAT")
        arcpy.CalculateField_management(stream_startend, 'XS_Station', 0, "VB")
        arcpy.AddField_management(stream_startend,'WSEL_REG',"FLOAT")
        arcpy.CalculateField_management(stream_startend, 'WSEL_REG', start_oid[0], "VB")
        arcpy.AddField_management(stream_startend,'Route_ID',"TEXT","","",50)
        arcpy.CalculateField_management(stream_startend, 'Route_ID', sqlExp2, "PYTHON")
        stream_start =[r for r in arcpy.da.SearchCursor (stream_startend,("Route_ID","WSEL_REG","XS_Station","Shape@XY"))]

        cursor = arcpy.da.InsertCursor(xs_pt, ("Route_ID","WSEL_REG","XS_Station","Shape@XY"))

        for row in stream_start:
            cursor.insertRow(row)
        fields = [f.name for f in arcpy.ListFields(xs_pt) if not f.required and f.name not in keep_fields ]
        arcpy.DeleteField_management(xs_pt, fields)
        return xs_pt


    def vertices_to_pts(self, feature,name):
        pts = arcpy.FeatureVerticesToPoints_management(feature, self.vertices_dataset+'/'+name+"_pts","ALL")
        self.verticies = arcpy.FeatureToPoint_management(pts,self.vertices_dataset+'/'+ name+"_vertices_feature","CENTROID")
        arcpy.Delete_management(pts)
        return self.verticies

    def processStream(self):
        all_streams = self.streams
        env.overwriteOutput = True
        for streams in all_streams:
            name = streams
            self.safe_print.print_out("Step 4 processing "+name)
            stream = self.streams_dataset+"\\"+name+"_stream_feature"
            xs = self.xs_dataset+'\\'+ name+'_xs'
            xs_intersect_pt = self.get_intersection(stream, xs, name)
            keep_fields = [f.name for f in arcpy.ListFields(stream)]
            routes = self.add_routes(stream, xs_intersect_pt, name, 0)
            streampt = self.vertices_to_pts(routes, name+'_stream')
            streamxy = arcpy.AddXY_management(streampt)
            dpts = arcpy.FeatureTo3DByAttribute_3d(streamxy, self.streams_zm+'/'+ name+"_pts_temp", 'POINT_M')
            dpts_clean = arcpy.FeatureToPoint_management(dpts, self.streams_zm+'/'+ name+"_pts_zm","CENTROID")
            arcpy.Delete_management(dpts)
            streamline = arcpy.PointsToLine_management(dpts_clean, self.streams_zm+'/'+ name+"_line_zm")
            updated_stream = arcpy.SpatialJoin_analysis(streamline, stream, self.streams_zm+'/'+ name+"_zm")
            fields = [f.name for f in arcpy.ListFields(updated_stream) if not f.required and f.name not in keep_fields ]
            arcpy.DeleteField_management(updated_stream, fields)
            routes = self.add_routes(updated_stream,xs_intersect_pt, name, 1)
            streampt = self.vertices_to_pts(routes, name+'_stream')
            streamxy = arcpy.AddXY_management(streampt)
            streamline = arcpy.PointsToLine_management(streamxy, self.streams_zm+'/'+ name+"_line_zm")
            updated_stream = arcpy.SpatialJoin_analysis(streamline, stream, self.routes_dataset+"/"+name+"_stream_routes")
            arcpy.DeleteField_management(updated_stream, fields)
        return
