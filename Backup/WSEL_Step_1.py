from __future__ import print_function
import sys, os, re, arcinfo, arcpy
from arcpy import env


class WSEL_Step_1:
    
    def __init__(self, config, streams):        
        self.streams = streams
        self.config = config
        arcpy.CheckOutExtension("3D")
   
    def __enter__(self):
        self.scratchgdb = self.config['scratchgdb']
        self.xs_original = self.config['xs_original']
        self.xs_dataset = self.config['xs_dataset']
        self.streams_original = self.config['streams_original']
        self.xs_intersect_dataset = self.config['xs_intersect_dataset']
        self.routes_dataset = self.config['routes_dataset']
        self.streams_dataset = self.config['streams_dataset']
        self.vertices_dataset = self.config['vertices_dataset']
        self.streams_zm =self.config['streams_zm']
        env.workspace = self.scratchgdb
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self

    def get_intersection(self, stream, xs, name):
        inFeatures = [stream, xs]
        intersectOutput = self.xs_intersect_dataset+"/"+name+"_xs_pt"
        clusterTolerance = 0
        pt = arcpy.Intersect_analysis(inFeatures, intersectOutput, "ALL", "", "POINT")
        feature = arcpy.FeatureToPoint_management(pt, self.xs_intersect_dataset+"/"+name+"_xs_pt_feature","CENTROID")
        arcpy.Delete_management(pt)
        return feature

    
    def add_routes(self,stream,xs_pt,name,status):
        #print("Converting stream line to Polyline ZM")
        rid = "StrmName"
        pts = xs_pt
        if status == 1:
            mfield="Section"
        else:
            mfield ="WSEL"
        out_fc = self.routes_dataset+"/"+name+"_stream_routes"
        rts = stream
        routes = arcpy.CalibrateRoutes_lr (rts, rid, pts, rid, mfield, out_fc,"Distance","5","","NO_BEFORE","NO_AFTER","IGNORE","KEEP","NO_INDEX")
        return routes



    def add_xy(self, stream):
        #print("Adding XY")
        streamxy=arcpy.AddXY_management(stream)
        return streamxy

    def vertices_to_pts(self, feature,name):
        #print("Converting "+name+" vertices to points")
        pts = arcpy.FeatureVerticesToPoints_management(feature,self.vertices_dataset+'/'+name+"_pts","ALL")
        verticies = arcpy.FeatureToPoint_management(pts,self.vertices_dataset+'/'+ name+"_vertices_feature","CENTROID")
        arcpy.Delete_management(pts)
        return verticies

    def processStream(self):        
        for stream in self.streams:
            sep = '_'
            name = stream.split(sep, 1)[0]
            #print("Starting stream "+name)
            xs = self.xs_dataset+"\\"+name+"_xs"            
            stream =self.streams_dataset+"\\"+name+"_stream_feature"
            keep_fields = [f.name for f in arcpy.ListFields(stream)]
            xs_intersect_pt = self.get_intersection(stream, xs, name)            
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
        return self.streams
        
        
