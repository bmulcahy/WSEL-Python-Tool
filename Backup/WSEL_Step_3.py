from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env


class WSEL_Step_3:
    
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
        return self.warnings

    def remove_duplicate_pts(self, stream_intersects):
        #print("Removing duplicate intersection points keeping ones with higher WSEL")
        tempLayer = "intersectLayer"
        expression = """ "StrmName_1"='Delete' """
        fieldName = "Intersects"
        expression2 = "[StrmName]"
        expression3 = "[StrmName_1]"
        expression4 = "[POINT_M]"
        keep_fields=['SHAPE', 'OBJECTID', 'StrmName', 'Intersects','WSEL', 'Section','FID_streams_all']
        comb_intersect = stream_intersects
        compare =[]

        cursor = arcpy.SearchCursor(comb_intersect, ['OID@','StrmName', 'StrmName_1','FID_streams_all','WSEL', 'POINT_M'])
       
        for row in cursor:            
            compare.append([row.getValue('StrmName'),row.getValue('StrmName_1'),row.getValue('WSEL'),row.getValue('POINT_M'),row.getValue('FID_streams_all')])
        del cursor

        cursor = arcpy.UpdateCursor(comb_intersect, ['StrmName_1', 'StrmName','WSEL','POINT_M'])
        for row in cursor:
            intersect = row.getValue('StrmName_1')
            intersect_stream = row.getValue('StrmName')
            intersect_WSEL = row.getValue('WSEL')
            intersect_Station = int(row.getValue('POINT_M')) 
            for strm in compare:
                stream = strm[1]
                stream_name = strm[0]
                stream_WSEL = strm[2]
                stream_Station = int(strm[3])
                if intersect == stream_name and intersect_stream == stream and intersect_WSEL < stream_WSEL or intersect_Station < stream_Station :
                    row.setValue("StrmName_1","Delete")
                    cursor.updateRow(row)        
        del cursor
        arcpy.AddField_management(comb_intersect,fieldName,"TEXT","","",254)
        arcpy.AddField_management(comb_intersect,'Section',"DOUBLE")
        arcpy.CalculateField_management(comb_intersect, fieldName, expression2, "VB")
        arcpy.CalculateField_management(comb_intersect, "StrmName", expression3, "VB")
        arcpy.CalculateField_management(comb_intersect, "Section", expression4, "VB")
        arcpy.MakeFeatureLayer_management(comb_intersect, tempLayer)
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression)
        if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(tempLayer)
        fields = [f.name for f in arcpy.ListFields(comb_intersect) if not f.required and f.name not in keep_fields ]
        arcpy.DeleteField_management(comb_intersect, fields)
        return

    def update_xs(self, intersect_fc):
        #print("Updating XS with backwater WSEL")
        warning ={}
        error = 0
        env.workspace = self.xs_dataset
        xs_array = arcpy.ListFeatureClasses()
        cursor = arcpy.SearchCursor(intersect_fc, ['StrmName', 'Intersects','WSEL','Section'])
        compare =[]
        for row in cursor:
            name = row.getValue('StrmName')
            intersect_stream = row.getValue('Intersects')
            section = row.getValue('Section')
            if section != 0.001:
                compare.append([name,row.getValue('WSEL'),section])
            else:
                error =error+1
                intersection = {name:intersect_stream}
                warning.update(intersection)
                
        
        del cursor
        
        
        for strm in compare:
            xs_name= strm[0]+"_xs"
            xs_WSEL= strm[1]           
            if xs_name in xs_array:                
                cursor = arcpy.UpdateCursor(xs_name, ['StrmName','WSEL','WSEL_REG'])
                for row in cursor:
                    original_wsel = row.getValue('WSEL')                    
                    if original_wsel < xs_WSEL:
                        row.setValue("WSEL",xs_WSEL)
                        cursor.updateRow(row)
                
                del cursor
        env.workspace = self.scratchgdb
        if error != 0:
            return warning
        else:
            return 'null'

    def processStream(self):
        self.warnings=[]
        comb_intersect = self.scratchgdb+'\\streams_intersect_all_2'
        self.remove_duplicate_pts(comb_intersect)
        warning = self.update_xs(comb_intersect)
        if warning != 'null':
            self.warnings.append(warning)
        return self.warnings
        
