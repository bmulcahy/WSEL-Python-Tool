from __future__ import print_function
import sys, os, re, arcpy, traceback
from arcpy import env
from arcpy.sa import *
### Created by Brian Mulcahy##
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
        env.workspace = self.scratch
        env.parallelProcessingFactor = "4"
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return 

    def get_intersection(self, stream, xs, name):
        print("Getting Intersection between "+name+"'s Stream and XS files")
        inFeatures = [stream, xs]
        intersectOutput = self.xs_intersect_dataset+"/"+name+"_xs_pt"
        clusterTolerance = 0
        pt = arcpy.Intersect_analysis(inFeatures, intersectOutput, "ALL", clusterTolerance, "POINT")
        self.feature = arcpy.FeatureToPoint_management(pt, self.xs_intersect_dataset+"/"+name+"_xs_pt_feature","CENTROID")
        arcpy.Delete_management(pt)
        return self.feature

    def add_routes(self,stream,xs_pt,name,status):
        
        rid = "Route_ID"
        
        if status == 0:
            print("Adding Z-values to Polyline ZM")
            mfield="WSEL_REG"
            pts = xs_pt
        if status == 1:
            print("Adding M-values to Polyline ZM")
            mfield ="XS_Station"
            #need to add 0 station at beginning of line if there is none because I am a nice guy
            stationList = [r[0] for r in arcpy.da.SearchCursor (xs_pt, [mfield])]
            min_station = min(stationList)
            if min_station > 0:
                print("No Zero Station creating one before processing")
                pts = self.add_zero_station(stream, xs_pt,min_station,name)           
        
        out_fc = self.routes_dataset+"/"+name+"_stream_routes"
        rts = stream
        out_routes = self.routes_dataset+"/stream_measures"
        route_evt_layer_temp =name+"_evt_lyr"
        route_evt_layer= self.scratch+"\\"+name+"_evt"
        props = "RID POINT MEAS"
        out_table =self.table_folder+"\\route_loc.dbf"

        route_meas = arcpy.CreateRoutes_lr(rts, rid, out_routes,"LENGTH", "#", "#", "UPPER_LEFT",1,0,"IGNORE", "INDEX")        
        loc_features = arcpy.LocateFeaturesAlongRoutes_lr(pts, route_meas, rid, "0", out_table, props, 'FIRST', 'NO_DISTANCE','NO_ZERO','FIELDS')
        evt_lyr = arcpy.MakeRouteEventLayer_lr(route_meas, rid, loc_features, props, route_evt_layer_temp, "#",  "NO_ERROR_FIELD",  "NO_ANGLE_FIELD","NORMAL","ANGLE", "LEFT", "POINT")
        lyr = arcpy.SaveToLayerFile_management(evt_lyr, route_evt_layer, "RELATIVE")
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
        start_oid =[r[0] for r in arcpy.da.SearchCursor (xs_pt,("NEAR_FID"),where_clause=sqlExp)][0]        
        sqlExp3 = "{0} <> {1}".format(fieldName2, start_oid)
        arcpy.MakeFeatureLayer_management(stream_startend, temp_pt_Layer)
        arcpy.SelectLayerByAttribute_management(temp_pt_Layer, "NEW_SELECTION", sqlExp3)
        if int(arcpy.GetCount_management(temp_pt_Layer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(temp_pt_Layer)
        arcpy.AddField_management(stream_startend,'XS_Station',"FLOAT")        
        arcpy.CalculateField_management(stream_startend, 'XS_Station', 0, "VB")
        arcpy.AddField_management(stream_startend,'Route_ID',"TEXT","","",50)        
        arcpy.CalculateField_management(stream_startend, 'Route_ID', sqlExp2, "PYTHON")
        stream_start =[r for r in arcpy.da.SearchCursor (stream_startend,("Route_ID","XS_Station","Shape@XY","Shape@Z","Shape@M"))]      
        
        cursor = arcpy.da.InsertCursor(xs_pt, ("Route_ID","XS_Station","Shape@XY","Shape@Z","Shape@M"))

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
            print("Starting "+name)
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
        
