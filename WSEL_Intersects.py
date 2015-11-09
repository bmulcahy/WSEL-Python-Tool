from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env
from Safe_Print import Safe_Print

class WSEL_Intersects:

    def __init__(self, config):
        self.config = config

    def __enter__(self):
        self.scratchgdb = self.config['scratchgdb']
        self.streams_original = self.config['streams_original']
        self.streams_intersect_dataset = self.config['streams_intersect_dataset']
        self.streams_dataset = self.config['streams_dataset']
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


    def get_intersect_all(self,comb_streams):
        self.safe_print.print_out("Intersecting all streams")
        env.workspace = self.streams_dataset
        streams_intersect = []
        tempLayer = "streamLayer"
        streamLayer="streamsAll"
        expression = """ "Route_ID" = "Route_ID_1" """
        fieldName = "WSEL"
        stream_array = [fc for fc in arcpy.ListFeatureClasses() if fc.endswith('_stream_feature')]
        clusterTolerance = 0
        for stream in stream_array:
            sep = '_'
            name = stream.split(sep, 1)[0]
            self.safe_print.print_out("Intersecting "+name)
            expression2 = """ "Route_ID" <>"""+"'"+name+"'"
            arcpy.SelectLayerByAttribute_management(comb_streams, "NEW_SELECTION",expression2)
            outFeature = self.streams_intersect_dataset+"/"+name+'_pt_intersect'
            streams_intersect.append(outFeature)
            pt = arcpy.Intersect_analysis([stream,comb_streams], outFeature, "ALL", clusterTolerance, "POINT")
            arcpy.AddXY_management(pt)
            arcpy.AddField_management(pt, fieldName,"FLOAT",10,3)
            arcpy.MakeFeatureLayer_management(pt, tempLayer)
            arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression)

            if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
                arcpy.DeleteFeatures_management(tempLayer)

        env.workspace = scratchgdb
        return

    def processStream(self):
        comb_streams = self.scratchgdb+'\\streams_all'
        streams_layer = arcpy.MakeFeatureLayer_management(comb_streams,"streams_lyr")
        intersect=self.get_intersect_all(streams_layer)
        return intersect
