from __future__ import print_function
import sys, os, re, arcinfo, arcpy
from arcpy import env


class WSEL_Stream_Setup:
    
    def __init__(self, config, streams):        
        self.streams = streams
        self.config = config
        arcpy.CheckOutExtension("3D")
   
    def __enter__(self):
        self.scratchgdb = self.config['scratchgdb']
        self.streams_original = self.config['streams_original']
        self.streams_dataset = self.config['streams_dataset']
        self.rid_field = self.config['rid_field']
        env.workspace = self.scratchgdb
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self

    def processStream(self):        
        for stream in self.streams:            
            name = stream
            print("Configuring stream "+name)
            expression = "'"+name+"'"            
            stream =arcpy.CopyFeatures_management(self.streams_original+"\\"+name+"_stream_feature", self.streams_dataset+"\\"+name+"_stream_feature" )
            arcpy.AddField_management(stream, "strm_length", "FLOAT",10,3)
            arcpy.CalculateField_management(stream, "strm_length", "float(!SHAPE.LENGTH!)","PYTHON")
            arcpy.AddField_management(stream,"Route_ID","TEXT","","",50)
            arcpy.CalculateField_management(stream,"Route_ID",expression,"PYTHON")
        return self.streams
        
        
