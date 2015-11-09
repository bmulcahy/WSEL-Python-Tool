from __future__ import print_function
import sys, os, re, arcpy
from arcpy import env
from Safe_Print import Safe_Print

class WSEL_Step_3:

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
        return self.warnings

    def remove_duplicate_pts(self, stream_intersects):
        ###THIS MAY NOT BE NEEDED WITH THE NEW PROCESS OF INTERSECTING
        self.safe_print.print_out("Removing duplicate intersection points keeping ones with higher WSEL")
        tempLayer = "intersectLayer"
        expression = """ "Intersects"='Delete' """
        comb_intersect = stream_intersects
        compare =[]

        cursor = arcpy.SearchCursor(comb_intersect, ['Route_ID','Intersects','WSEL', 'XS_Section'])

        for row in cursor:
            compare.append([row.getValue('Route_ID'),row.getValue('Intersects'),row.getValue('WSEL'),row.getValue('XS_Section')])
        del cursor

        cursor = arcpy.UpdateCursor(comb_intersect, ['Intersects', 'Route_ID','WSEL','XS_Section'])
        for row in cursor:
            intersect = row.getValue('Intersects')
            intersect_stream = row.getValue('Route_ID')
            intersect_WSEL = row.getValue('WSEL')
            for strm in compare:
                stream = strm[1]
                stream_name = strm[0]
                stream_WSEL = strm[2]
                if intersect == stream_name and intersect_stream == stream and intersect_WSEL < stream_WSEL:
                    #print(intersect_stream+": "+str(intersect_WSEL)+" "+stream_name+": "+str(stream_WSEL))
                    row.setValue("Intersects","Delete")
                    cursor.updateRow(row)
        del cursor

        arcpy.MakeFeatureLayer_management(comb_intersect, tempLayer)
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION",expression)
        if int(arcpy.GetCount_management(tempLayer).getOutput(0)) > 0:
            arcpy.DeleteFeatures_management(tempLayer)

        return

    def update_xs(self, intersect_fc, xs_name):
        self.safe_print.print_out("Updating All XS's with backwater WSEL")
        warning ={}
        error = 0
        env.workspace = self.xs_dataset
        #xs_array = arcpy.ListFeatureClasses()
        cursor = arcpy.SearchCursor(intersect_fc, ['Route_ID', 'Intersects','WSEL','XS_Section'])
        compare =[]
        

        for row in cursor:
            name = row.getValue('Route_ID')
            intersect_stream = row.getValue('Intersects')
            section = row.getValue('XS_Section')
            if section != 0 and name == xs_name:
                compare.append([name,row.getValue('WSEL'),section])
            else:
                error =error+1
                intersection = {name:intersect_stream}
                warning.update(intersection)

        del cursor
        
        for strm in compare:
            xs_name= strm[0]+"_xs"
            xs_WSEL= strm[1]
            cursor = arcpy.UpdateCursor(xs_name, ['Route_ID','WSEL','WSEL_REG','Backwater'])
            for row in cursor:
                original_wsel = row.getValue('WSEL')
                if original_wsel < xs_WSEL:
                    row.setValue("WSEL_REG",xs_WSEL)
                    row.setValue("Backwater","yes")
                    cursor.updateRow(row)
            del cursor
        env.workspace = self.scratchgdb
        return warning
        

    def processStream(self):
        self.warnings=[]
        all_streams = self.streams        
        for streams in all_streams:
            comb_intersect = self.scratchgdb+'\\streams_intersect_all_2'
            self.remove_duplicate_pts(comb_intersect)
            warning = self.update_xs(comb_intersect, streams)
            #self.update_xs(comb_intersect, streams)
            if warning != 'null':
                self.warnings.append(warning)
        return self.warnings
