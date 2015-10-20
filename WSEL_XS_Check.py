from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env


class WSEL_XS_Check:
    
    def __init__(self, config, streams):        
        self.streams = streams
        self.config = config
   
    def __enter__(self):
        self.scratchgdb = self.config['scratchgdb']
        self.xs_original = self.config['xs_original']
        self.xs_dataset = self.config['xs_dataset']
        self.streams_original = self.config['streams_original']
        self.xs_intersect_dataset = self.config['xs_intersect_dataset']
        self.routes_dataset = self.config['routes_dataset']
        self.streams_dataset = self.config['streams_dataset']
        self.vertices_dataset = self.config['vertices_dataset']
        self.wsel_field =self.config['wsel_field']
        self.station_field =self.config['station_field']
        self.rid_field =self.config['rid_field']
        env.workspace = self.scratchgdb
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self.warnings

    def FieldExist(featureclass, fieldname):
        fieldList = arcpy.ListFields(featureclass, fieldname)
        fieldCount = len(fieldList)
        if (fieldCount == 1):
            return True
        else:
            return False

    def xs_check(self, xs, name):
        warning ={name:[]}
        expression = "["+self.wsel_field+"]"
        expression2 = "["+self.station_field+"]"
        expression3 = "["+self.rid_field+"]"
         
        arcpy.AddField_management(xs,'Valid',"Double")
        arcpy.AddField_management(xs,'WSEL',"FLOAT")
        arcpy.AddField_management(xs,'Station',"FLOAT")
        arcpy.AddField_management(xs,'Route_ID',"TEXT","","",254)
        arcpy.CalculateField_management(xs, 'WSEL', expression, "VB")
        arcpy.CalculateField_management(xs, 'Station', expression2, "VB")
        arcpy.CalculateField_management(xs, 'Valid', "0", "VB")

        if (not FieldExist(xs, self.rid_field)):
            stream= self.streams_dataset+"\\"+name+"_stream_feature"
            stream_rid =[r[0] for r in arcpy.da.SearchCursor (stream,("Route_ID"))][0]
            arcpy.CalculateField_management(xs, 'Route_ID', stream_rid, "VB")
            
        else:
            arcpy.CalculateField_management(xs, 'Route_ID', expression3, "VB")
        cursor = arcpy.UpdateCursor(xs, fields='Valid; WSEL; Station',sort_fields="Station A")
        count = arcpy.GetCount_management(xs).getOutput(0)        
        i=0
        error = 0
        prevrow =''
        for row in cursor:
            wsel = row.getValue('WSEL')
            section =row.getValue('Station')
            if section == 0:               
               row.setValue("Valid",1)
               row.setValue("Station",0.001)               
               cursor.updateRow(row) 
            if i == 0:
                prevrow = wsel                
            if i != 0:                
                previous = prevrow
                if previous> wsel:
                    error = error + 1
                    section = row.getValue('Station')
                    #print("Section: " + str(section) + " invalid")
                    row.setValue("Valid",1)
                    row.setValue("WSEL",previous+0.001)
                    warning[name].append(section)
                    cursor.updateRow(row)                    
                wsel = row.getValue('WSEL')
                prevrow = wsel
            i=i+1
        del row
        del cursor        
        if error != 0:
            return warning
        else:
            return 'null'
        

    def processStream(self):
        self.warnings=[]
        for stream in self.streams:
            sep = '_'
            name = stream.split(sep, 1)[0]
            #print("Starting stream "+name)
            xs = arcpy.FeatureToLine_management(self.xs_original+"\\"+name+"_xs", self.xs_dataset+"\\"+name+"_xs")
            warning = self.xs_check(xs, name)
            if warning != 'null':
                self.warnings.append(warning)
            #print("Finished stream "+name)            
        return self.warnings
        
        
