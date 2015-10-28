from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env
from Safe_Print import Safe_Print

class WSEL_Step_2:

    def __init__(self, config, streams):
        self.config = config
        self.streams = streams

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
        self.multi=self.config['multiproc']
        self.modelbuilder=self.config['modelbuilder']
        self.print_config = {'multi': self.multi, 'modelbuilder': self.modelbuilder}
        self.safe_print = Safe_Print(self.print_config)
        env.workspace = self.scratchgdb
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self


    def get_intersect_all(self,comb_streams,name):
        self.safe_print.print_out("Intersecting all streams")
        env.workspace = self.routes_dataset        
        
        tempLayer = "streamLayer"

        keep_fields =['Route_ID','WSEL','Intersects','XS_Section']
        expression = """ "Route_ID" = "Intersects" """
        expression2 = "[POINT_Z]"
        expression4 = "[Route_ID]"
        expression5 = "[Route_ID_1]"
        expression6 = "[POINT_M]"

        
        stream = self.routes_dataset +'\\'+name+'_stream_routes'
        clusterTolerance = 0.01        
        self.safe_print.print_out("Intersecting "+name)              
        outFeature = self.streams_intersect_dataset+"/"+name+'_pt_intersect'        
        pt = arcpy.Intersect_analysis([comb_streams,stream], outFeature, "ALL", clusterTolerance, "POINT")
        arcpy.AddXY_management(pt)
        arcpy.AddField_management(pt, "WSEL", "FLOAT",10,3)
        arcpy.AddField_management(pt,'Intersects',"TEXT","","",50)
        arcpy.AddField_management(pt,'XS_Section',"FLOAT",10,3)
        arcpy.CalculateField_management(pt, "WSEL", expression2, "VB")
        arcpy.CalculateField_management(pt, "Intersects", expression4, "VB")
        arcpy.CalculateField_management(pt, "Route_ID", expression5, "VB")
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
        all_streams = self.streams
        env.overwriteOutput = True
        for streams in all_streams:
            name = streams
            comb_streams = self.scratchgdb+'\\streams_all'
            streams_layer = arcpy.MakeFeatureLayer_management(comb_streams,"streams_lyr")
            self.get_intersect_all(streams_layer,name)
        return
