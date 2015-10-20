from __future__ import print_function
import sys, os, re, arcpy, traceback
from arcpy import env
from arcpy.sa import *

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
        env.scratchWorkspace = self.scratchgdb
        env.parallelProcessingFactor = "0"
        env.overwriteOutput = True
        env.MResolution = 0.0001
        env.MDomain = "0 10000000"
        env.outputMFlag = "Enabled"
        env.outputZFlag = "Enabled"
        return self

    def __exit__(self, type, value, traceback):
        return self.result

    def points_to_tin(self, points, xs_lines, name):
         
        out_raster = self.output_workspace+name
        #print("Converting elevation points to Tin")        
        tin = self.tin_folder+"\\tin_"+name
        heightfield = "POINT_Z"
        xs_height ="WSEL"
        projection = arcpy.SpatialReference(self.sr)         
        tin_out = arcpy.CreateTin_3d(tin, projection, [[points, heightfield , "Mass_Points"],[xs_lines,xs_height,"hardline"]], "CONSTRAINED_DELAUNAY")
        #print("Converting Tin to Raster")
        raster = arcpy.TinRaster_3d(tin_out, out_raster, "FLOAT", "LINEAR", "CELLSIZE 3", 1)       
        return raster

    def raster_extract(self, raster, name):        
        
        boundary = self.flood_original+"\\"+name+"_flood_boundary"
        #print("Clipping Raster to Flood Boundary")            
        outExtractByMask = ExtractByMask(raster, boundary)
        outExtractByMask.save(self.output_workspace+name)
        return

    def processStream(self):
        all_streams = self.streams
        self.result =[]
        for streams in all_streams:
            sep = '_'
            name = streams.split(sep, 1)[0]
            name = streamstr[0]+'_'+streamstr[1]
            print("Starting "+name)           
            stream_vertices = self.vertices_dataset+'/'+name+"_stream_vertices_feature"
            xs = self.xs_dataset+'/'+name+"_xs"
            raster = self.points_to_tin(stream_vertices ,xs, name)
            self.raster_extract(raster, name)            
            print("Finished Step 5 for "+name)
        return self.result
        
