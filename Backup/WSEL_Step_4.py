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
        print("Converting stream line to Polyline ZM")
        rid = "StrmName"
        pts = xs_pt
        if status == 0:
            mfield="WSEL_REG"
        if status == 1:
            mfield ="Section"
        if status == 2:
            mfield ="WSEL_REG"
        out_fc = self.routes_dataset+"/"+name+"_stream_routes"
        rts = stream
        out_routes = self.routes_dataset+"/stream_measures"
        route_evt_layer_temp =name+"_evt_lyr"
        route_evt_layer= self.scratch+"\\"+name+"_evt"
        props = "RID POINT MEAS"
        out_table =self.table_folder+"\\route_loc"

        route_meas = arcpy.CreateRoutes_lr(rts, rid, out_routes,"LENGTH", "#", "#", "UPPER_LEFT",1,0,"IGNORE", "INDEX")        
        loc_features = arcpy.LocateFeaturesAlongRoutes_lr(pts, route_meas, rid, "0", out_table, props, 'FIRST', 'NO_DISTANCE','NO_ZERO','FIELDS')
        evt_lyr = arcpy.MakeRouteEventLayer_lr(route_meas, rid, loc_features, props, route_evt_layer_temp, "#",  "NO_ERROR_FIELD",  "NO_ANGLE_FIELD","NORMAL","ANGLE", "LEFT", "POINT")
        lyr = arcpy.SaveToLayerFile_management(evt_lyr, route_evt_layer, "RELATIVE")
        routes = arcpy.CalibrateRoutes_lr (route_meas, rid, lyr, rid, mfield, out_fc,"MEASURES","0","BETWEEN","NO_BEFORE","NO_AFTER","IGNORE","KEEP","INDEX")
        arcpy.Delete_management(loc_features)
        arcpy.Delete_management(route_meas)
        arcpy.Delete_management(lyr)
        return routes

    def fix_measures(self,intersects,stream_pts,name):
        print("Fixing Measures")
        tempLayer = "stream_vertLayer"
        expression = '"StrmName" = ' + "'" + name + "'" 
        expression2 = """ "StrmName" = 'Delete' """
        new_wsel = 0
        
        cursor = arcpy.SearchCursor(intersects,where_clause=expression, fields='StrmName; WSEL; Section')
        for row in cursor:
            str_name = row.getValue('StrmName')
            wsel = row.getValue('WSEL')
            if str_name == name and row.getValue('Section') != 0.001:
                new_wsel = wsel       
              
        del cursor           

        cursor = arcpy.da.UpdateCursor(stream_pts,['StrmName','WSEL_REG','Section','POINT_Z','POINT_M','SHAPE@Z'])
        for row in cursor:            
            str_name = row[0]
            wsel = row[1]
            pointm = row[2]
            shapez= round(row[3],4) 
            shapem= round(row[4],4)        
            if wsel < new_wsel or pointm == 0.001:
                geom = row[5]
                row[5] = new_wsel
                row[1] = new_wsel    
                cursor.updateRow(row)                
            if pointm == wsel or pointm == 0:
                row[0] = 'Delete'                
                cursor.updateRow(row)            
            if shapez == shapem:
                row[0] = 'Delete'  
                cursor.updateRow(row)
        
        del cursor

        arcpy.MakeFeatureLayer_management(stream_pts, tempLayer)
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression2)

        if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(tempLayer)       
            
        return stream_pts

   


    def vertices_to_pts(self, feature,name):        
        pts = arcpy.FeatureVerticesToPoints_management(feature, self.vertices_dataset+'/'+name+"_pts","ALL")
        self.verticies = arcpy.FeatureToPoint_management(pts,self.vertices_dataset+'/'+ name+"_vertices_feature","CENTROID")     
        arcpy.Delete_management(pts)
        return self.verticies    
  
    def stream_clean(self, feature, name):
        print("Cleaning up file")
        fieldName = "WSEL_REG"        
        expression = "[POINT_Z]"
        fieldName2 = "Section"        
        expression2 = "[POINT_M]"
        arcpy.AddField_management(feature, fieldName, "DOUBLE")
        arcpy.CalculateField_management(feature, fieldName, expression, "VB")
        arcpy.AddField_management(feature, "Section", "DOUBLE")
        arcpy.CalculateField_management(feature, fieldName2, expression2, "VB")
        return feature

    def processStream(self):
        all_streams = self.streams
        env.overwriteOutput = True
        intersect_tbl = self.scratchgdb+"/streams_intersect_all_2"
        for streams in all_streams:
            sep = '_'
            name = streams.split(sep, 1)[0]
            #print("Starting "+name)
            stream = arcpy.CopyFeatures_management(self.streams_original+"\\"+name+"_stream_feature", self.streams_dataset+"\\"+name+"_stream_feature" )
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
            stream_vertices = self.stream_clean(streamxy,name)
            new_vert = self.fix_measures(intersect_tbl,stream_vertices,name)
            streamline = arcpy.PointsToLine_management(new_vert, self.routes_dataset+"/"+name+"_stream_routes")
        return 
        
