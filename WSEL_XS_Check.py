from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env
from safe_print import Safe_Print

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
        self.backwater = self.config['backwater']
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
        return self.warnings

    def FieldExist(self,featureclass, fieldname):
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
        expression3 = "'"+name+"'"

        arcpy.AddField_management(xs,'Valid',"Double")
        arcpy.AddField_management(xs,'WSEL',"FLOAT",10,3)
        arcpy.AddField_management(xs,'WSEL_REG',"FLOAT",10,3)
        arcpy.AddField_management(xs,'XS_Station',"FLOAT",10,3)
        arcpy.AddField_management(xs,'Route_ID',"TEXT","","",254)

        arcpy.CalculateField_management(xs, 'WSEL', expression, "VB")
        arcpy.CalculateField_management(xs, 'WSEL_REG', expression, "VB")
        arcpy.CalculateField_management(xs, 'XS_Station', expression2, "VB")
        arcpy.CalculateField_management(xs, 'Valid', "0", "VB")
        arcpy.CalculateField_management(xs, 'Route_ID', expression3, "PYTHON")

        if self.backwater == True:
            arcpy.AddField_management(xs,'Backwater',"TEXT","","",6)
            arcpy.CalculateField_management(xs, 'Backwater', "'no'", "PYTHON")


        cursor = arcpy.UpdateCursor(xs, fields='Valid; WSEL; XS_Station',sort_fields="XS_Station A")
        count = arcpy.GetCount_management(xs).getOutput(0)
        i=0
        error = 0
        prevrow =''
        for row in cursor:
            wsel = row.getValue('WSEL_REG')
            section =row.getValue('XS_Station')
            if section == 0:
               row.setValue("Valid",1)
               row.setValue("XS_Station",0.001)
               cursor.updateRow(row)
            if i == 0:
                prevrow = wsel
            if i != 0:
                previous = prevrow
                if previous> wsel:
                    error = error + 1
                    section = row.getValue('XS_Station')
                    self.safe_print.print_out("Section: " + str(section) + " invalid")
                    row.setValue("Valid",1)
                    row.setValue("WSEL",previous+0.001)
                    row.setValue("WSEL_REG",previous+0.001)
                    warning[name].append(section)
                    cursor.updateRow(row)
                wsel = row.getValue('WSEL_REG')
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
            name = stream
            self.safe_print.print_out("XS Check processing stream "+name)
            xs = arcpy.FeatureToLine_management(self.xs_original+"\\"+name+"_xs", self.xs_dataset+"\\"+name+"_xs")
            warning = self.xs_check(xs, name)
            if warning != 'null':
                self.warnings.append(warning)
            
        return self.warnings
