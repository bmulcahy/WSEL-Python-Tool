from __future__ import print_function
import sys, os, re, arcpy, traceback
from arcpy import env
from arcpy.sa import *
from safe_print import Safe_Print

class WSEL_Step_5:

    def __init__(self, config, streams):
        self.streams = streams
        self.config = config

    def __enter__(self):
        arcpy.CheckOutExtension("3D")
        arcpy.CheckOutExtension("Spatial")
        self.scratch = self.config['scratch']
        self.scratchgdb = self.config['scratchgdb']
        self.xs_original = self.config['xs_original']
        self.output_workspace = self.config['output_workspace']
        self.xs_dataset = self.config['xs_dataset']
        self.streams_original = self.config['streams_original']
        self.flood_original =self.config['flood_original']
        self.xs_intersect_dataset = self.config['xs_intersect_dataset']
        self.streams_intersect_dataset = self.config['streams_intersect_dataset']
        self.routes_dataset = self.config['routes_dataset']
        self.streams_dataset = self.config['streams_dataset']
        self.vertices_dataset = self.config['vertices_dataset']
        self.sr = self.config['sr']
        self.tin_folder=self.config['tin_folder']
        self.multi=self.config['multiproc']
        self.modelbuilder=self.config['modelbuilder']
        self.backwater=self.config['backwater']
        self.flood_boundary=self.config['flood_boundary']
        self.flood_dataset=self.config['flood_dataset']
        self.wsel_field=self.config['wsel_field']
        self.print_config = {'multi': self.multi, 'modelbuilder': self.modelbuilder}
        self.safe_print = Safe_Print(self.print_config)
        env.scratchWorkspace = self.scratchgdb
        env.parallelProcessingFactor = "4"
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self.result

    def points_to_tin(self, points, xs_lines, name):

        out_raster = self.output_workspace+name+'_'+self.wsel_field
        self.safe_print.print_out("Converting "+name+" elevation points to Tin")
        tin = self.tin_folder+"\\tin_"+name
        heightfield = "POINT_Z"
        xs_height ="WSEL_REG"
        projection = arcpy.SpatialReference(self.sr)
        tin_out = arcpy.CreateTin_3d(tin, projection, [[points, heightfield , "Mass_Points"],[xs_lines,xs_height,"hardline"]], "CONSTRAINED_DELAUNAY")
        self.safe_print.print_out("Converting "+name+" Tin to Raster")
        raster = arcpy.TinRaster_3d(tin_out, out_raster, "FLOAT", "LINEAR", "CELLSIZE 3", 1)
        return raster

    def backwater_correction(self, points, xs_lines, name):
        sqlexp ="{0}={1}".format("Backwater", "'no'")
        sqlexp3="Shape_Area"
        sql_intersect ="{0}={1}".format("Route_ID", "'"+name+"'")
        sql_raster ="{0}={1}".format("Overlap", "'no'")
        out_raster = self.output_workspace+name+'_'+self.wsel_field
        keep_fields = ["Overlap","flood_area","flood_main"]
        boundary = self.flood_original+"\\"+name+"_flood_boundary"


        intersect_name = [r[0] for r in arcpy.da.SearchCursor (self.scratchgdb+'\\streams_intersect_all_2', ["Intersects"],sql_intersect)]
        avail_intersect = len(intersect_name)        
        if avail_intersect>0 and self.flood_boundary == True:            
            intersect_bound =  self.flood_original+"\\"+intersect_name[0]+"_flood_boundary"
        
        
        temp_bound = self.flood_dataset+"\\"+name+"_flood_temp"
        flood_bound = self.flood_dataset+"\\"+name+"_boundary"
        dis_bound =self.flood_dataset+"\\"+name+"_flood_dis"
        erase1 =self.flood_dataset+"\\"+name+"_flood_erase1"
        erase2 =self.flood_dataset+"\\"+name+"_flood_erase2"

        
        pts_layer = arcpy.MakeFeatureLayer_management (points, "pts")
        xs_layer = arcpy.MakeFeatureLayer_management (xs_lines, "xs")
        
        arcpy.Near_analysis(pts_layer, xs_layer)
        arcpy.AddJoin_management(pts_layer,"NEAR_FID",xs_layer,"OBJECTID")
        arcpy.SelectLayerByAttribute_management(xs_layer,"CLEAR_SELECTION",sqlexp)
        arcpy.SelectLayerByAttribute_management(pts_layer,"CLEAR_SELECTION",sqlexp)
        #arcpy.SelectLayerByAttribute_management(xs_layer,"NEW_SELECTION",sqlexp)
        #arcpy.SelectLayerByAttribute_management(pts_layer,"NEW_SELECTION",sqlexp)
        #if int(arcpy.GetCount_management(xs_layer).getOutput(0)) <= 0:
            #arcpy.SelectLayerByAttribute_management(xs_layer,"CLEAR_SELECTION",sqlexp)
            #arcpy.SelectLayerByAttribute_management(pts_layer,"CLEAR_SELECTION",sqlexp)
            

        tin = self.tin_folder+"\\tin_"+name
        heightfield = name+"_stream_vertices_feature.POINT_Z"
        xs_height ="WSEL_REG"

        projection = arcpy.SpatialReference(self.sr)
        #THIS COMMENTED OUT CODE WOULD CREATE A TIN FROM THE XS AND PTS USE THIS AS THE BEGINNING OF CREATING A FLOOD POLYGON FROM
        #SCRATCH. WILL NEED TO ADD LOGIC FOR SUBTRACTING LIDAR ELEV
        #if self.lidar == True:
            #tin_out = arcpy.CreateTin_3d(tin, projection, [[pts_layer, heightfield , "Mass_Points"],[xs_layer,xs_height,"hardline"]], "CONSTRAINED_DELAUNAY")
            #raster = arcpy.TinRaster_3d(tin_out, out_raster, "INT", "LINEAR", "CELLSIZE 3", 1)
            #arcpy.RasterToPolygon_conversion(raster, temp_bound, "NO_SIMPLIFY")
            #arcpy.Dissolve_management(temp_bound,dis_bound,"#","#","SINGLE_PART")
        if self.flood_boundary == True:
            if avail_intersect != 0:
                arcpy.AddField_management(boundary, "Overlap", "TEXT",4)
                arcpy.CalculateField_management(boundary, "Overlap", "'no'","PYTHON")
                arcpy.Erase_analysis(boundary, intersect_bound, erase1)
                arcpy.Erase_analysis(boundary,erase1,erase2)
                arcpy.CalculateField_management(erase2, "Overlap", "'yes'","PYTHON")
                arcpy.Merge_management([erase1,erase2],temp_bound)
                arcpy.Delete_management(erase1)
                arcpy.Delete_management(erase2)
            else:
                arcpy.CopyFeatures_management(boundary,temp_bound)
                arcpy.AddField_management(temp_bound, "Overlap", "TEXT",4)
                arcpy.CalculateField_management(temp_bound, "Overlap", "'no'","PYTHON")
            arcpy.MultipartToSinglepart_management(temp_bound,flood_bound)
            arcpy.AddField_management(flood_bound, "flood_area", "FLOAT",10,3)
            arcpy.CalculateField_management(flood_bound, "flood_area", "float(!SHAPE.AREA!)","PYTHON")
            arcpy.AddField_management(flood_bound, "flood_main", "TEXT",4)
            arcpy.CalculateField_management(flood_bound, "flood_main", "'no'","PYTHON")
            temp_poly =arcpy.CopyFeatures_management(flood_bound,self.flood_dataset+"\\"+name+"_flood_boundary")
        
            areaList = [r[0] for r in arcpy.da.SearchCursor (flood_bound, ["flood_area"])]
            if len(areaList)>0:
                max_area = max(areaList)            
                sqlexp2 ="{0}<>{1}".format("flood_area", max_area)
                arcpy.MakeFeatureLayer_management (temp_poly, "flood_temp")
                arcpy.SelectLayerByAttribute_management("flood_temp","NEW_SELECTION",sqlexp2)
                arcpy.CalculateField_management("flood_temp", "flood_main", "'yes'","PYTHON")
                #if int(arcpy.GetCount_management("flood_temp").getOutput(0)) > 0:
                    #arcpy.DeleteFeatures_management("flood_temp")
            arcpy.Delete_management(temp_bound)
            #arcpy.Delete_management(dis_bound)
            arcpy.Delete_management(flood_bound)
            fields = [f.name for f in arcpy.ListFields(temp_poly) if not f.required and f.name not in keep_fields ]
            arcpy.DeleteField_management(temp_poly, fields)        
        tin_out = arcpy.CreateTin_3d(tin, projection, [[pts_layer, heightfield , "masspoints"],[xs_layer,xs_height,"hardline"]], "CONSTRAINED_DELAUNAY")
        raster = arcpy.TinRaster_3d(tin_out, out_raster, "FLOAT", "LINEAR", "CELLSIZE 1.5", 1)
        if self.flood_boundary == True:
            self.safe_print.print_out("Clipping "+name+"'s raster to Flood Boundary")
            arcpy.MakeFeatureLayer_management(temp_poly, "flood_temp")
            #arcpy.SelectLayerByAttribute_management("flood_temp","NEW_SELECTION",sql_raster)#This will clip the boundary being overlapped by the stream it is flowing into
            outExtractByMask = ExtractByMask(raster, "flood_temp")
            outExtractByMask.save(self.output_workspace+name+'_'+self.wsel_field)        
        return
    
    def raster_extract(self, raster, name):
        boundary = self.flood_original+"\\"+name+"_flood_boundary"
        self.safe_print.print_out("Clipping "+name+"'s raster to Flood Boundary")
        outExtractByMask = ExtractByMask(raster, boundary)
        outExtractByMask.save(self.output_workspace+name+'_'+self.wsel_field)
        return

    def processStream(self):
        all_streams = self.streams
        self.result =[]
        for streams in all_streams:
            name = streams            
            self.safe_print.print_out("Step 5 processing "+name)
            stream_vertices = self.vertices_dataset+'/'+name+"_stream_vertices_feature"
            xs = self.xs_dataset+'/'+name+"_xs"
            if self.backwater == True:
                self.backwater_correction(stream_vertices ,xs, name)
            else:
                raster = self.points_to_tin(stream_vertices ,xs, name)
                if self.flood_boundary == True:
                    self.raster_extract(raster, name)
                self.safe_print.print_out("Finished Step 5 for "+name)
        return
