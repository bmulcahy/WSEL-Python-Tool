from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env


class WSEL_Intersects_Clean:
    
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
        return 

    def remove_duplicate_pts(self, stream_intersects):
        #print("Removing duplicate intersection points keeping ones with higher WSEL")
        tempLayer = "intersectLayer"
        expression = """ "Route_ID_1"='Delete' """        
        expression2 = "[Route_ID]"
        expression3 = "[Route_ID_1]"
        expression4 = "[POINT_M]"
        keep_fields=['SHAPE', 'OBJECTID', 'Route_ID', 'Intersects','WSEL', 'Station','strm_length']
        comb_intersect = stream_intersects
        compare =[]
        fields = [f.name for f in arcpy.ListFields(comb_intersect)]       

        cursor = arcpy.SearchCursor(comb_intersect, ['Route_ID','Route_ID_1','strm_length'])        
        for row in cursor:            
            compare.append([row.getValue('Route_ID'),row.getValue('Route_ID_1'),row.getValue('strm_length')])
        del row
        del cursor      
        

        cursor = arcpy.UpdateCursor(comb_intersect,['Route_ID','Route_ID_1','strm_length'])
        for row in cursor:
            intersect = row.getValue('Route_ID_1')
            intersect_stream = row.getValue('Route_ID')
            intersect_length = int(row.getValue('strm_length'))    
            
                       
            for strm in compare:
                stream = strm[1]
                stream_name = strm[0]                
                stream_length = int(strm[2])
                if intersect == stream_name and intersect_stream == stream and intersect_length < stream_length:
                    row.setValue("Route_ID_1","Delete")
                    cursor.updateRow(row)
        del row
        del cursor
        arcpy.AddField_management(comb_intersect,"Intersects","TEXT","","",50)
        arcpy.AddField_management(comb_intersect,'Station',"FLOAT",10,3)
        arcpy.CalculateField_management(comb_intersect, "Intersects", expression2, "VB")
        arcpy.CalculateField_management(comb_intersect, "Route_ID", expression3, "VB")
        arcpy.CalculateField_management(comb_intersect, "Station", expression4, "VB")
        arcpy.MakeFeatureLayer_management(comb_intersect, tempLayer)
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression)
        if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(tempLayer)
        fields = [f.name for f in arcpy.ListFields(comb_intersect) if not f.required and f.name not in keep_fields ]
        arcpy.DeleteField_management(comb_intersect, fields)
        return

    

    def processStream(self):
        self.warnings=[]
        comb_intersect = self.scratchgdb+'\\streams_intersect_all_1'
        self.remove_duplicate_pts(comb_intersect)       
        
        return 
        
