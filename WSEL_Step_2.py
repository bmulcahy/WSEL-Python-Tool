from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env


class WSEL_Step_2:

    def __init__(self, config):
        self.config = config

    def __enter__(self):
        self.scratchgdb = self.config['scratchgdb']
        self.xs_original = self.config['xs_original']
        self.xs_dataset = self.config['xs_dataset']
        self.streams_original = self.config['streams_original']
        self.xs_intersect_dataset = self.config['xs_intersect_dataset']
        self.streams_intersect_dataset = self.config['streams_intersect_dataset']
        self.routes_dataset = self.config['routes_dataset']
        self.streams_dataset = self.config['streams_dataset']
        self.vertices_dataset = self.config['vertices_dataset']
        env.workspace = self.scratchgdb
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self


    def get_intersect_all(self,comb_streams):
        #print("Intersecting all streams")
        env.workspace = self.routes_dataset
        streams_intersect = []
        streamLayer="streamsAll"
        tempLayer = "streamLayer"
        expression = """ "Route_ID" = "Route_ID_1" """
        keep_fields =['Route_ID', 'Route_ID_1','WSEL', 'POINT_M','Intersects','XS_Section']        
        expression2 = "[POINT_Z]"

        expression4 = "[Route_ID]"
        expression5 = "[Route_ID_1]"
        expression6 = "[POINT_M]"
        
        stream_array = [fc for fc in arcpy.ListFeatureClasses() if fc.endswith('_stream_routes')]
        
        clusterTolerance = 0.01
        for stream in stream_array:
            sep = '_'
            name = stream.split(sep, 1)[0]
            expression3 = """ "Route_ID" <>"""+"'"+name+"'"
            print("Intersecting "+name)
            arcpy.MakeFeatureLayer_management(comb_streams, streamLayer)
            arcpy.SelectLayerByAttribute_management(streamLayer, "NEW_SELECTION",expression3)
            outFeature = self.streams_intersect_dataset+"/"+name+'_pt_intersect'
            streams_intersect.append(outFeature)
            pt = arcpy.Intersect_analysis([stream,streamLayer], outFeature, "ALL", clusterTolerance, "POINT")
            arcpy.AddXY_management(pt)
            arcpy.AddField_management(pt, "WSEL", "FLOAT",10,3)            
            arcpy.AddField_management(pt,'Intersects',"TEXT","","",50)
            arcpy.AddField_management(pt,'XS_Section',"FLOAT",10,3)
            arcpy.CalculateField_management(pt, "WSEL", expression2, "VB")
            arcpy.CalculateField_management(pt, "Intersects", expression4, "VB")
            #arcpy.CalculateField_management(pt, "Route_ID", expression5, "VB")
            arcpy.CalculateField_management(pt, "XS_Section", expression6, "VB")
            fields = [f.name for f in arcpy.ListFields(pt) if not f.required and f.name not in keep_fields ]
            arcpy.DeleteField_management(pt, fields)
            arcpy.MakeFeatureLayer_management(pt, tempLayer)
            arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression)
            if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
                arcpy.DeleteFeatures_management(tempLayer)                
            
        env.workspace = scratchgdb
        return   

    def processStream(self):
        comb_streams = self.scratchgdb+'\\streams_all'
        streams_layer = arcpy.MakeFeatureLayer_management(comb_streams,"streams_lyr")
        self.get_intersect_all(streams_layer)
        return 
        
        
