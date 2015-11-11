from __future__ import print_function
import time, sys, os, re, arcinfo, arcpy, cPickle, logging, json
from multiprocessing import Pool
from WSEL_XS_Check import *
from WSEL_Stream_Setup import *
from WSEL_Intersects import *
from WSEL_Intersects_Clean import *
from WSEL_Step_1 import *
from WSEL_Step_2 import *
from WSEL_Step_3 import *
from WSEL_Step_4 import *
from WSEL_Step_5 import *
from arcpy import env



def Script_setup(check, scriptlocation, r):
   setup={}
   if check==True:
      #TODO add support for arcgis toolbox
      multiproc = arcpy.GetParameterAsText(1)
      proc = arcpy.GetParameterAsText(2)
      projectname = arcpy.GetParameterAsText(3)
      rootdir = arcpy.GetParameterAsText(4)
      sr=arcpy.GetParameterAsText(5)
      modelbuilder = True
   else:
      multiproc = True ##For large groups you must use this
      proc = 4
      modelbuilder = False
      main_stream = "CottonwoodRiver" #ignored if not doing backwater
      flood_boundary = True ##If using for polylinezm only flood_boundary is not needed
      rid_field="StrmName"
      wsel_field = r
      station_field ="Section"
      backwater = True
      projectname = "Cottonwood_run1245"
      rootdir = "C:\\Users\\bmulcahy\\External\\Projects\\WSEL-Python-Tool\\data\\run1245"
      sr="NAD 1983 UTM Zone 14N"
   main =os.path.join(scriptlocation,"output\\"+projectname)
   scratch = os.path.join(main,wsel_field)
   if not os.path.exists(scratch):
      os.makedirs(scratch)
   configfile = os.path.join(scratch,"config.cfg")
   logfile = os.path.join(scratch,"log.txt")
   originalgdb = os.path.join(main,"Original.gdb")
   streams_original = os.path.join(originalgdb,'streams')
   flood_original = os.path.join(originalgdb,'flood')
   xs_original = os.path.join(originalgdb,'xs')
   finalgdb = os.path.join(main,"Final.gdb")
   streams_final = os.path.join(finalgdb,'streams')
   flood_final = os.path.join(finalgdb,'flood')
   xs_final = os.path.join(finalgdb,'xs')
   final = os.path.join(main,"Output")
   comb = os.path.join(scratch,"combined_output")
   combinedgdb = os.path.join(comb,"Comb.gdb")
   combined_workspace = os.path.join(comb,"Comb.gdb\\")
   comb_rast = os.path.join(combinedgdb,projectname)
   setup ={'flood_boundary':flood_boundary,'modelbuilder': modelbuilder,
           'main_stream':main_stream,'main': main,
           'finalgdb': finalgdb,'streams_final': streams_final,
           'flood_final':flood_final,'xs_final':xs_final,
           'rid_field': rid_field,'wsel_field': wsel_field,
           'station_field': station_field,'backwater': backwater,
           'sr': sr,'Rootdir':rootdir,
           'Projectname': projectname, 'multiproc': multiproc,
           'Proc': proc,'scratch': scratch,
           'logfile': logfile,'configfile': configfile,
           'originalgdb': originalgdb,'streams_original': streams_original,
           'flood_original':flood_original,'xs_original':xs_original,
           'final':final,'comb':comb,
           'combinedgdb':combinedgdb,'combined_workspace':combined_workspace,
           'comb_rast':comb_rast}

   return setup


def print_to_log(setup, param, data):
   logfile = setup['logfile']
   with open(logfile, "ab+") as text_file:
      print("{} = {}".format(param,data), end='\n',file=text_file)
   return

def print_to_config(setup, param, data):
   configfile = setup['configfile']
   topickle = {param: data}
   with open(configfile, "a+b") as text_file:
      cPickle.dump(topickle,text_file)
   return

def OriginalWorkspace(setup):
   print("Creating workspace")
   main=setup['main']
   scratch = setup['scratch']
   final= setup['final']
   comb= setup['comb']
   combinedgdb= setup['combinedgdb']
   originalgdb= setup['originalgdb']
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   flood_original= setup['flood_original']
   finalgdb= setup['finalgdb']
   xs_final= setup['xs_final']
   streams_final= setup['streams_final']
   flood_final= setup['flood_final']
   comb_rast= setup['comb_rast']
   configfile = setup['configfile']
   sr = setup['sr']
   projectname = setup['Projectname']


   if not os.path.exists(final):
      os.makedirs(final)
   if not os.path.exists(comb):
      os.makedirs(comb)
   if not os.path.exists(combinedgdb):
      arcpy.CreateFileGDB_management(comb, "Comb.gdb")
   if not os.path.exists(finalgdb):
      arcpy.CreateFileGDB_management(main, "Final.gdb")
   if not arcpy.Exists(xs_final):
      arcpy.CreateFeatureDataset_management(finalgdb, "xs", sr)
   if not arcpy.Exists(streams_final):
      arcpy.CreateFeatureDataset_management(finalgdb, "streams", sr)
   if not arcpy.Exists(flood_final):
      arcpy.CreateFeatureDataset_management(finalgdb, "flood", sr)
   if not os.path.exists(originalgdb):
      arcpy.CreateFileGDB_management(main, "Original.gdb")
   if not arcpy.Exists(xs_original):
      arcpy.CreateFeatureDataset_management(originalgdb, "xs", sr)
   if not arcpy.Exists(streams_original):
      arcpy.CreateFeatureDataset_management(originalgdb, "streams", sr)
   if not arcpy.Exists(flood_original):
      arcpy.CreateFeatureDataset_management(originalgdb, "flood", sr)
   if not arcpy.Exists(comb_rast):
      arcpy.CreateRasterCatalog_management(combinedgdb, projectname)
   else:
      arcpy.DeleteRasterCatalogItems_management(comb_rast)

   env.workspace = originalgdb
   env.overwriteOutput = True

   env.MResolution = 0.0001
   env.MDomain = "0 10000000"
   env.outputMFlag = "Enabled"
   env.outputZFlag = "Enabled"
   print_to_log(setup,"OriginalWorkspace","Complete")
   print_to_config(setup,"OriginalWorkspace",True)
   return

def CopyWorkspace(setup):

   
   print("Copying data into workspace")
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   streams_dataset= setup['streams_original']
   flood_original= setup['flood_original']
   rootdir = setup['Rootdir']
   job_config ={'config':[],'stream_names':[]}
   flood_boundary = setup['flood_boundary']
   incomplete_streams =[]
   stream_count = 0
   for name in os.listdir(rootdir):
      if os.path.isdir(os.path.join(rootdir, name)):
         directory = os.path.join(rootdir, name)
         stream_loc = os.path.join(directory, name+'.shp')
         xs_original_loc = os.path.join(directory, name+'_xsecs_results.shp')
         boundary_loc = os.path.join(directory, name+'_flood.shp')
         if flood_boundary == True:
            if os.path.isfile(stream_loc) and os.path.isfile(xs_original_loc) and os.path.isfile(boundary_loc):
               print("Copying "+name+" data into file geodatabase")
               stream_count=stream_count+1
               job_config['stream_names'].append(name)
               arcpy.FeatureToLine_management(xs_original_loc, xs_original+"/"+name+"_xs")
               arcpy.FeatureToLine_management(stream_loc,streams_original+"/"+name+"_stream_feature")
               arcpy.FeatureToPolygon_management(boundary_loc,flood_original+"/"+name+"_flood_boundary")
            else:
               incomplete_streams.append(name)
               print(name+" does not have all the needed data")
         elif os.path.isfile(stream_loc) and os.path.isfile(xs_original_loc):
            print("Copying "+name+" data into file geodatabase")
            stream_count=stream_count+1
            job_config['stream_names'].append(name)
            arcpy.FeatureToLine_management(xs_original_loc, xs_original+"/"+name+"_xs")
            arcpy.FeatureToLine_management(stream_loc,streams_original+"/"+name+"_stream_feature")            
         else:
            incomplete_streams.append(name)
            print(name+" does not have all the needed data")
            
   print(str(stream_count)+" Streams found that can be processed.")
   missing=''.join(incomplete_streams)
   if incomplete_streams:
      print_to_log(setup,"Incomplete Data for Streams(folder names)",missing)
   print_to_log(setup,"Number of Streams to be processed",stream_count)
   print_to_log(setup,"CopyWorkspace","Complete")
   print_to_config(setup,"CopyWorkspace",True)
   print_to_config(setup,"StreamCount",stream_count)
   print_to_config(setup,"JobConfig_1",job_config)
   return (stream_count,job_config)

def ScratchWorkspace(setup, stream_count, job_config, proc):
   main = setup['main']
   scratch = setup['scratch']
   final= setup['final']
   comb= setup['comb']
   combinedgdb= setup['combinedgdb']
   originalgdb= setup['originalgdb']
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   flood_original= setup['flood_original']
   finalgdb= setup['finalgdb']
   xs_final= setup['xs_final']
   streams_final= setup['streams_final']
   flood_final= setup['flood_final']
   comb_rast= setup['comb_rast']
   configfile = setup['configfile']
   sr = setup['sr']
   projectname = setup['Projectname']
   wsel_field = setup['wsel_field']
   station_field = setup['station_field']
   rid_field = setup['rid_field']
   backwater = setup['backwater']
   multi = setup['multiproc']
   modelbuilder = setup['modelbuilder']
   flood_boundary =setup['flood_boundary']

   if stream_count < proc:
      proc = stream_count
   if multi == True:
      for p in range(proc):
         i = p+1
         print("Creating workspace for processor "+str(i))
         scratchoriginal = scratch
         scratchproc = os.path.join(scratchoriginal,"proc"+str(i))
         scratchgdb = os.path.join(scratchproc,"Scratch.gdb")
         finalgdb = os.path.join(main,"Final.gdb")
         tablefolder = os.path.join(scratchproc,"Tables")
         output_workspace = os.path.join(main,"Final.gdb\\")
         raster_catalog = os.path.join(finalgdb,projectname)
         streams_dataset = os.path.join(scratchgdb,'streams')
         streams_intersect_dataset = os.path.join(scratchgdb,'streams_intersect')
         xs_intersect_dataset = os.path.join(scratchgdb,'xs_intersect')
         vertices_dataset = os.path.join(scratchgdb,'vertices')
         routes_dataset = os.path.join(scratchgdb,'routes')
         xs_dataset = os.path.join(scratchgdb,'xs')
         flood_dataset = os.path.join(scratchgdb,'flood')
         streams_zm = os.path.join(scratchgdb,'streams_zm')
         tin_folder = os.path.join(scratchproc,"Tins")
         if not os.path.exists(scratchproc):
            os.makedirs(scratchproc)
         if not os.path.exists(comb):
            os.makedirs(comb)
         if not os.path.exists(tablefolder):
            os.makedirs(tablefolder)
         if not os.path.exists(tin_folder):
            os.makedirs(tin_folder)
         if not os.path.exists(scratchgdb):
            arcpy.CreateFileGDB_management(scratchproc, "Scratch.gdb")
         if not os.path.exists(finalgdb):
            arcpy.CreateFileGDB_management(main, "Final.gdb")
         if not arcpy.Exists(streams_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "streams", sr)
         if not arcpy.Exists(streams_intersect_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "streams_intersect", sr)
         if not arcpy.Exists(xs_intersect_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "xs_intersect", sr)
         if not arcpy.Exists(vertices_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "vertices", sr)
         if not arcpy.Exists(routes_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "routes", sr)
         if not arcpy.Exists(xs_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "xs", sr)
         if not arcpy.Exists(flood_dataset):
            arcpy.CreateFeatureDataset_management(scratchgdb, "flood", sr)
         if not arcpy.Exists(streams_zm):
            arcpy.CreateFeatureDataset_management(scratchgdb, "streams_zm", sr)
         if not arcpy.Exists(raster_catalog):
            arcpy.CreateRasterCatalog_management(finalgdb, projectname, arcpy.SpatialReference(sr))
         else:
            arcpy.DeleteRasterCatalogItems_management(raster_catalog)

         job_config['config'].append({'flood_boundary':flood_boundary,'modelbuilder':modelbuilder,'multiproc':multi,
                                      'backwater': backwater,'rid_field': rid_field,
                                      'finalgdb': finalgdb,'streams_final': streams_final,
                                      'flood_final':flood_final,'xs_final':xs_final,
                                      'wsel_field': wsel_field, 'station_field': station_field,
                                      'table_folder':tablefolder,'tin_folder':tin_folder,
                                      'configfile': configfile,'streams_zm':streams_zm,
                                      'scratch': scratchproc,'sr': sr,
                                      'originalgdb': originalgdb, 'scratchgdb':scratchgdb,
                                      'finalgdb':finalgdb,'output_workspace':output_workspace,
                                      'raster_catalog':raster_catalog,'streams_dataset':streams_dataset,
                                      'streams_intersect_dataset':streams_intersect_dataset,'xs_intersect_dataset':xs_intersect_dataset,
                                      'vertices_dataset':vertices_dataset,'routes_dataset':routes_dataset,
                                      'xs_dataset':xs_dataset,'streams_original':streams_original,
                                      'xs_original':xs_original,'flood_original':flood_original,
                                      'flood_dataset':flood_dataset})

   else:
      scratchgdb = os.path.join(scratch,"Scratch.gdb")
      finalgdb = os.path.join(main,"Final.gdb")
      tablefolder = os.path.join(scratch,"Tables")
      output_workspace = os.path.join(main,"Final.gdb\\")
      raster_catalog = os.path.join(finalgdb,projectname)
      streams_dataset = os.path.join(scratchgdb,'streams')
      streams_intersect_dataset = os.path.join(scratchgdb,'streams_intersect')
      xs_intersect_dataset = os.path.join(scratchgdb,'xs_intersect')
      vertices_dataset = os.path.join(scratchgdb,'vertices')
      routes_dataset = os.path.join(scratchgdb,'routes')
      xs_dataset = os.path.join(scratchgdb,'xs')
      flood_dataset = os.path.join(scratchgdb,'flood')
      streams_zm = os.path.join(scratchgdb,'streams_zm')
      tin_folder = os.path.join(scratch,"Tins")
      if not os.path.exists(tin_folder):
         os.makedirs(tin_folder)
      if not os.path.exists(scratchgdb):
         arcpy.CreateFileGDB_management(scratch, "Scratch.gdb")
      if not os.path.exists(finalgdb):
         arcpy.CreateFileGDB_management(main, "Final.gdb")
      if not os.path.exists(tablefolder):
         os.makedirs(tablefolder)
      if not arcpy.Exists(streams_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "streams", sr)
      if not arcpy.Exists(streams_intersect_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "streams_intersect", sr)
      if not arcpy.Exists(xs_intersect_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "xs_intersect", sr)
      if not arcpy.Exists(vertices_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "vertices", sr)
      if not arcpy.Exists(routes_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "routes", sr)
      if not arcpy.Exists(xs_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "xs", sr)
      if not arcpy.Exists(flood_dataset):
         arcpy.CreateFeatureDataset_management(scratchgdb, "flood", sr)
      if not arcpy.Exists(streams_zm):
         arcpy.CreateFeatureDataset_management(scratchgdb, "streams_zm", sr)
      if not os.path.exists(raster_catalog):
         arcpy.CreateRasterCatalog_management(finalgdb, projectname)
      else:
         arcpy.DeleteRasterCatalogItems_management(raster_catalog)
      job_config['config'].append({'flood_boundary':flood_boundary,
                                   'modelbuilder':modelbuilder,'multiproc':multi,
                                   'backwater': backwater,'rid_field': rid_field,
                                   'finalgdb': finalgdb,'streams_final': streams_final,
                                   'flood_final':flood_final,'xs_final':xs_final,
                                   'wsel_field': wsel_field, 'station_field': station_field,
                                   'table_folder':tablefolder,'tin_folder':tin_folder,
                                   'configfile': configfile,'streams_zm':streams_zm,
                                   'scratch':scratch,'sr':sr,
                                   'originalgdb': originalgdb,'scratchgdb':scratchgdb,
                                   'finalgdb':finalgdb,'output_workspace':output_workspace,
                                   'raster_catalog':raster_catalog,'streams_dataset':streams_dataset,
                                   'streams_intersect_dataset':streams_intersect_dataset,'xs_intersect_dataset':xs_intersect_dataset,
                                   'vertices_dataset':vertices_dataset,'routes_dataset':routes_dataset,
                                   'xs_dataset':xs_dataset,'streams_original':streams_original,
                                   'xs_original':xs_original,'flood_original':flood_original,
                                   'flood_dataset':flood_dataset})
   print_to_log(setup,"Proc",proc)
   print_to_config(setup,"Proc",proc)
   print_to_config(setup,"JobConfig",job_config)
   print_to_log(setup,"ScratchWorkspace","Complete")
   print_to_config(setup,"ScratchWorkspace",True)
   return (proc, job_config)

def WSEL_XSCheck(streamJobs):
   config=streamJobs['config']
   stream_names = streamJobs['stream_names']
   with WSEL_XS_Check(config,stream_names) as wsel_XS_Check:
      result=wsel_XS_Check.processStream()
      return result

def WSEL_StreamSetup(streamJobs):
   config=streamJobs['config']
   stream_names = streamJobs['stream_names']
   with WSEL_Stream_Setup(config,stream_names) as wsel_StreamSetup:
      result=wsel_StreamSetup.processStream()
      return result

def WSEL_IntersectJob(streamJobs):
   config=streamJobs['config']
   stream_names = streamJobs['stream_names']
   with WSEL_Intersects(config) as wsel_Intersects:
      result=wsel_Intersects.processStream()
      return result

def WSEL_IntersectsClean(streamJobs):
   config=streamJobs['config']
   with WSEL_Intersects_Clean(config) as wsel_Intersects_Clean:
      result=wsel_Intersects_Clean.processStream()
      return result

def WSEL_step1(streamJobs):
   config=streamJobs['config']
   stream_names = streamJobs['stream_names']
   with WSEL_Step_1(config,stream_names) as wsel_Step_1:
      result=wsel_Step_1.processStream()
      return result

def WSEL_step2(streamJobs):
    config=streamJobs['config']
    stream_names = streamJobs['stream_names']
    with WSEL_Step_2(config, stream_names) as wsel_Step_2:
       wsel_Step_2.processStream()
       return

def WSEL_step3(streamJobs):
    config=streamJobs['config']
    stream_names = streamJobs['stream_names']
    with WSEL_Step_3(config, stream_names) as wsel_Step_3:
       result=wsel_Step_3.processStream()
       return result

def WSEL_step4(streamJobs):
    config=streamJobs['config']
    stream_names = streamJobs['stream_names']
    with WSEL_Step_4(config, stream_names) as wsel_Step_4:
       wsel_Step_4.processStream()
       return

def WSEL_step5(streamJobs):
    config=streamJobs['config']
    stream_names = streamJobs['stream_names']
    with WSEL_Step_5(config, stream_names) as wsel_Step_5:
       wsel_Step_5.processStream()
       return

def remove_dup(seq):
    # Order preserving
    return list(_remove_dup(seq))

def _remove_dup(seq):
    seen = set()
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        yield x

def merge(merge_stream,all_intersects,main_list,name):
   new_merge={name:[]}
   for stream in main_list:
      if all_intersects.has_key(stream):
         p=len(new_merge[name])
         new_merge[name].append({stream:[]})
         for trib in all_intersects[stream]:
            if all_intersects.has_key(trib):
               new_merge[name][p][stream].append({trib:all_intersects[trib]})
               i=len(new_merge[name][p][stream])
               merge(new_merge[name][p][stream][i-1],all_intersects,all_intersects[trib],trib)
            else:
               new_merge[name][p][stream].append(trib)

      else:
         new_merge[name].append(stream)

   merge_stream.update(new_merge)
   return merge_stream

def flatten_stream(stream_dict, stream_list):
   new_Streamlist=stream_list

   if isinstance(stream_dict, dict):
      for stream in stream_dict.keys():
         new_Streamlist.append(stream)
      for values in stream_dict.values():
         for i in range(0,len(values)):
            if isinstance(values[i], dict):
               flatten_stream(values[i],new_Streamlist)
            else:
               new_Streamlist.append(values[i])
   return stream_list

def stream_order(setup,streamJobs):
   print("Creating list of streams for processing order")
   stream_dict = {}
   stream_initdict ={}
   intersectList=[]
   strmList=[]
   main=setup['main_stream']
   scratchgdb_loc = streamJobs[0]['config']['scratchgdb']
   intersect_table = scratchgdb_loc+'\\streams_intersect_all_1'
   strmList_raw=[r for r in arcpy.da.SearchCursor(intersect_table, ['Route_ID','Intersects','strm_length'])]
   for r in strmList_raw:
      strmList.append([r[0],r[1]])
      intersectList.append(r[1])

   intersectList.insert(0,main)
   intersectList_uniq=remove_dup(intersectList)

   for strm in intersectList_uniq:
      stream_initdict[strm] =[]
   for strm in strmList:
      stream_initdict[strm[1]].append(strm[0])

   main_strm =stream_initdict.pop(main)
   stream_dict=merge({main: main_strm},stream_initdict,main_strm, main)
   stream_order = flatten_stream(stream_dict, list([]))
   print(json.dumps(stream_order))
   print_to_config(setup,"stream_list",stream_order)
   return stream_order

def getConfig(stream,streamJobs,procs):
   for i in range(0, procs):
      if stream in streamJobs[i]['stream_names']:
         return i

def finalize_data(setup,streamJobs,proc,multi):
   multiproc = multi
   xs_final=setup['xs_final']
   finalgdb = setup['finalgdb'] 
   streams_final = setup['streams_final']
   flood_final = setup['flood_final']
   wsel_field=setup['wsel_field']
   
   print("Moving Data")
   if multiproc == True:
      for p in range(proc):
         stream_loc = streamJobs[p]['config']['routes_dataset']
         xs_loc = streamJobs[p]['config']['xs_dataset']
         bound_loc = streamJobs[p]['config']['flood_dataset']
         env.workspace = stream_loc
         env.overwriteOutput = True
         streams = arcpy.ListFeatureClasses()
         for name in streams:
            arcpy.CopyFeatures_management(name,streams_final+'\\'+name+'_'+wsel_field)
         env.workspace = xs_loc
         env.overwriteOutput = True
         xs = arcpy.ListFeatureClasses()
         for name in xs:
            arcpy.CopyFeatures_management(name,xs_final+'\\'+name+'_'+wsel_field)
         env.workspace = bound_loc
         env.overwriteOutput = True
         boundary = arcpy.ListFeatureClasses()
         for name in boundary:
            arcpy.CopyFeatures_management(name,flood_final+'\\'+name+'_'+wsel_field)
         stream_loc = streams_final        
         env.workspace = stream_loc
         env.overwriteOutput = True
         streams = arcpy.ListFeatureClasses()         
         streams_all = arcpy.Merge_management(streams, finalgdb+"\\streams_all"+'_'+wsel_field)
         xs_loc = xs_final        
         env.workspace = xs_loc
         env.overwriteOutput = True
         xs = arcpy.ListFeatureClasses()         
         xs_all = arcpy.Merge_management(xs, finalgdb+"\\xs_all"+'_'+wsel_field)
   else:
      stream_loc = streamJobs[0]['config']['routes_dataset']
      xs_loc = streamJobs[0]['config']['xs_dataset']
      env.workspace = stream_loc
      streams = arcpy.ListFeatureClasses()
      for name in streams:
         arcpy.CopyFeatures_management(name,streams_final+'\\'+name+'_'+wsel_field)
      env.workspace = xs_loc
      xs = arcpy.ListFeatureClasses()
      for name in xs:
         arcpy.CopyFeatures_management(name,xs_final+'\\'+name+'_'+wsel_field)
      stream_loc = streams_final        
      env.workspace = stream_loc
      env.overwriteOutput = True
      streams = arcpy.ListFeatureClasses()         
      streams_all = arcpy.Merge_management(streams, finalgdb+"\\streams_all_"+wsel_field)
      xs_loc = xs_final        
      env.workspace = xs_loc
      env.overwriteOutput = True
      xs = arcpy.ListFeatureClasses()
      xs_all = arcpy.Merge_management(xs,finalgdb+"\\xs_all"+'_'+wsel_field)
   xs_fields =['Route_ID','WSEL','Intersects','XS_Section']
   fields = [f.name for f in arcpy.ListFields(xs_all) if not f.required and f.name not in xs_fields ]
   arcpy.DeleteField_management(xs_all, fields)
   stream_fields =['Route_ID']
   fields = [f.name for f in arcpy.ListFields(streams_all) if not f.required and f.name not in stream_fields ]
   arcpy.DeleteField_management(streams_all, fields)
   print_to_config(setup,"finalize_data",True)
   print("Data moved to Final Geodatabase")
   return

def StreamJobs(setup,config,procs):
   
   multi = setup['multiproc']
   streamarr=config['stream_names']
   if(len(streamarr)<=procs):
      p=1
   else:
      p=int(len(streamarr)/procs)
   jobsList=[]
   streamSet=[]
   t = 0
   if multi == True:
      for i in streamarr:
         if(len(streamSet)+2<=p):
            streamSet.append(i)
         elif(len(jobsList)<procs):
            streamSet.append(i)
            configSet={'stream_names':streamSet,'config':config['config'][t]}
            t=t+1
            jobsList.append(configSet)
            streamSet=[]
         else:
            streamSet.append(i)
      j=0
      if(len(streamSet)>0): #distribute the remaining streams across the existing jobs
         for i in range(0,len(streamSet)):
            if(j<len(jobsList)):
               jobsList[j]['stream_names'].append(streamSet[i])
               j=j+1
            else:
               j=0
   else:
      streamSet = streamarr
      configSet={'stream_names':streamSet,'config':config['config'][0]}
      jobsList.append(configSet)



   print_to_config(setup,"StreamJobs",jobsList)
   return jobsList


def MergeStreams(setup,streamJobs, proc, run,multi):
   print("Merging Streams")
   multiproc = multi
   print(multiproc)
   stream_list =[]
   if multiproc  == True:
      for p in range(proc):
         if run == 1:
            stream_loc = streamJobs[p]['config']['streams_dataset']
         else:
            stream_loc = streamJobs[p]['config']['routes_dataset']
         scratchgdb_loc = streamJobs[p]['config']['scratchgdb']
         env.workspace = stream_loc
         env.overwriteOutput = True
         streams = arcpy.ListFeatureClasses()
         if len(streams)>0:
            env.workspace = scratchgdb_loc
            env.overwriteOutput = True
            local_streams = arcpy.Merge_management(streams, "local_streams_all")
            stream_list.append(scratchgdb_loc+'\\local_streams_all')
      for p in range(proc):
         scratchgdb_loc = streamJobs[p]['config']['scratchgdb']
         env.workspace = scratchgdb_loc
         env.overwriteOutput = True
         comb_streams = arcpy.Merge_management(stream_list, "streams_all")
   else:
      if run == 1:
         stream_loc = streamJobs[0]['config']['streams_dataset']
      else:
         stream_loc = streamJobs[0]['config']['routes_dataset']
      scratchgdb_loc = streamJobs[0]['config']['scratchgdb']
      env.workspace = stream_loc
      env.overwriteOutput = True
      streams = arcpy.ListFeatureClasses()
      env.workspace = scratchgdb_loc
      env.overwriteOutput = True
      comb_streams = arcpy.Merge_management(streams, "streams_all")
   if run!=2:
      print_to_log(setup,"MergeStreams_"+str(run),"Complete")
      print_to_config(setup,"MergeStreams_"+str(run),True)
   print("Merge Streams completed")
   return

def MergeIntersects(setup,streamJobs,proc,run, multi):
   stream_intersect =[]
   multiproc  = multi
   print(multiproc)
   run=run
   print("Merging Stream Intersects")
   if multiproc  == True:
      for p in range(proc):
         stream_loc =streamJobs[p]['config']['streams_intersect_dataset']
         scratchgdb_loc = streamJobs[p]['config']['scratchgdb']
         env.workspace = stream_loc
         streams = arcpy.ListFeatureClasses()
         print(str(len(streams)))
         if len(streams)>0:
            env.workspace = scratchgdb_loc
            local_streams = arcpy.Merge_management(streams, "local_streams_intersect_all_"+str(run))
            stream_intersect.append(scratchgdb_loc+'\\local_streams_intersect_all_'+str(run))         
      for p in range(proc):         
         scratchgdb_loc = streamJobs[p]['config']['scratchgdb']
         env.workspace = scratchgdb_loc
         env.overwriteOutput = True
         comb_intersect = arcpy.Merge_management(stream_intersect, scratchgdb_loc+"\\streams_intersect_all_"+str(run))
         
   else:
      stream_loc = streamJobs[0]['config']['streams_intersect_dataset']
      scratchgdb_loc = streamJobs[0]['config']['scratchgdb']
      env.workspace = stream_loc
      streams = arcpy.ListFeatureClasses()      
      comb_intersect = arcpy.Merge_management(streams, scratchgdb_loc+"\\streams_intersect_all_"+str(run))
      env.workspace = scratchgdb_loc
   if run!=2:
      print_to_log(setup,"MergeIntersects_"+str(run),"Complete")
      print_to_config(setup,"MergeIntersects_"+str(run),True)
   print("Merge Intersects completed")
   return

def comb_raster(setup,streamJobs,proc,multi):
   comb_rast =setup['comb_rast']
   final = setup['final']
   projectname = setup['Projectname']
   wsel_field=setup['wsel_field']
   multiproc=multi
   print("Combining rasters")
   raster_loc = streamJobs[0]['config']['output_workspace']
   raster_cat = streamJobs[0]['config']['raster_catalog']
   arcpy.WorkspaceToRasterCatalog_management(raster_loc, raster_cat,"INCLUDE_SUBDIRECTORIES","PROJECT_ONFLY")
   arcpy.RasterCatalogToRasterDataset_management(raster_cat,os.path.join(final,projectname+'_'+wsel_field+".tif"),"",
                                                    "MAXIMUM", "FIRST","", "", "32_BIT_FLOAT")
   #if multiproc == True:
      #for p in range(proc):         
         #raster_loc = streamJobs[p]['config']['output_workspace']
         #raster_cat = streamJobs[p]['config']['raster_catalog']
         #arcpy.WorkspaceToRasterCatalog_management(raster_loc, raster_cat,"INCLUDE_SUBDIRECTORIES","PROJECT_ONFLY")
      #arcpy.RasterCatalogToRasterDataset_management(raster_cat,os.path.join(final,projectname+'_'+wsel_field+".tif"),"",
                                                    #"MAXIMUM", "FIRST","", "", "32_BIT_FLOAT")
   #else:
      #raster_loc = streamJobs[0]['config']['output_workspace']
      #raster_cat = streamJobs[0]['config']['raster_catalog']
      #arcpy.WorkspaceToRasterCatalog_management(raster_loc, raster_cat,"INCLUDE_SUBDIRECTORIES","PROJECT_ONFLY")
      #arcpy.RasterCatalogToRasterDataset_management(raster_cat,os.path.join(final,projectname+'_'+wsel_field+".tif"),"",
                                                    #"MAXIMUM", "FIRST","", "", "32_BIT_FLOAT")
   print_to_config(setup,"comb_raster",True)
   print("All water surface elevations rasters have been created")

def XSCheck(setup,proc,streamJobs):
   warning ={}
   error = 0
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning XS Check using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_XSCheck(job)
         #if result != None:
            #warning.update(result[0])
      pool=Pool(processes=proc)
      result = pool.map(WSEL_XSCheck,streamJobs)
      pool.close()
      pool.join()
      for i in result[0]:
         if i != None:
            error = error +1
            warning.update(i)

   else:
      print("Beginning XS Check without multiprocesser module")
      result=WSEL_XSCheck(streamJobs[0])
      if result != None:
         error = error +1
         print(result)
         warning.update(result[0])

   print("XS have been checked")
   print(warning)
   if error >0:
      warning_string=json.dumps(warning)
      print_to_log(setup,"XSWarnings",warning_string)
   print_to_log(setup,"XSCheck","Complete")
   print_to_config(setup,"XSCheck",True)
   return

def StreamSetup(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning Stream Setup using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_StreamSetup(job)
      pool=Pool(processes=proc)
      result = pool.map(WSEL_StreamSetup,streamJobs)
      pool.close()
      pool.join()
   else:
      print("Beginning Stream Setup without multiprocesser module")
      WSEL_StreamSetup(streamJobs[0])
   print("Stream Setup completed")
   print_to_log(setup,"Stream Setup","Complete")
   print_to_config(setup,"StreamSetup",True)
   return

def IntersectJob(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning Intersects using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_StreamSetup(job)
      pool=Pool(processes=proc)
      result = pool.map(WSEL_IntersectJob,streamJobs)
      pool.close()
      pool.join()
   else:
      print("Beginning Intersects without multiprocesser module")
      WSEL_IntersectJob(streamJobs[0])
   print("Stream Intersects completed")
   print_to_log(setup,"Intersects","Complete")
   print_to_config(setup,"Intersects",True)
   return

def IntersectsClean(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning Intersects Clean using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_IntersectsClean(job)
      pool=Pool(processes=proc)
      result = pool.map(WSEL_IntersectsClean,streamJobs)
      pool.close()
      pool.join()
   else:
      print("Beginning Intersects Clean without multiprocesser module")
      WSEL_IntersectsClean(streamJobs[0])
   print("Stream Intersects Clean completed")
   print_to_log(setup,"IntersectsClean","Complete")
   print_to_config(setup,"IntersectsClean",True)
   return

def Intersects_delete(setup,streamJobs,proc):
   stream_intersect =[]
   multi = streamJobs[0]['config']['multiproc']
   print("Deleting Initial Stream Intersects")
   if multi == True:
      for p in range(proc):
         stream_loc = streamJobs[p]['config']['streams_intersect_dataset']
         scratchgdb_loc = streamJobs[p]['config']['scratchgdb']
         env.workspace = stream_loc
         streams = arcpy.ListFeatureClasses()
         env.workspace = scratchgdb_loc
         for fc in streams:
            arcpy.Delete_management(fc)
   else:
      stream_loc = streamJobs[0]['config']['streams_intersect_dataset']
      scratchgdb_loc = streamJobs[0]['config']['scratchgdb']
      env.workspace = stream_loc
      streams = arcpy.ListFeatureClasses()
      env.workspace = scratchgdb_loc
      for fc in streams:
         arcpy.Delete_management(fc)
   print_to_log(setup,"IntersectsDelete","Complete")
   print_to_config(setup,"IntersectsDelete",True)
   print("Initial Intersects deletion completed")
   return

def Step1(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning Step 1")
      #for job in streamJobs:
         #result = WSEL_step1(job)
      pool=Pool(processes=proc)
      result = pool.map(WSEL_step1,streamJobs)
      pool.close()
      pool.join()
   else:
      print("Beginning Step 1")
      WSEL_step1(streamJobs[0])
   print("Step 1 completed")
   print("Streams have been configured")
   return


def Step2(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   if multi == True:
      print("Beginning Step 2")
      #for job in streamJobs:
         #result = WSEL_step2(job)
      pool=Pool(processes=proc)
      result = pool.map(WSEL_step2,streamJobs)
      pool.close()
      pool.join()
   else:
      print("Beginning Step 2")
      WSEL_step2(streamJobs[0])
   print("Step 2 completed")
   return

def Step3(setup,proc,streamJobs):
   multi = streamJobs[0]['config']['multiproc']
   error = 0
   warning ={}
   if multi == True:
      print("Beginning Step 3")
      #for job in streamJobs:
         #result = WSEL_step3(job)
         #if result != None:
            #warning.update(result[0])
      pool=Pool(processes=proc)
      result = pool.map(WSEL_step3,streamJobs)
      pool.close()
      pool.join()


      #warning.update(result)
   else:
      print("Beginning Step 3")
      result=WSEL_step3(streamJobs[0])
      #warning.update(result)
   if error >0:
      warning_string=json.dumps(warning)
      print_to_log(setup,"Stream Intersect Warning",warning_string)
   print(warning)
   print("Step 3 completed")
   return

def Step4(setup,proc,streamJobs, multiproc):#still does not like multi processing
   multi = multiproc
   if multi == True:
      print("Beginning Step 4")
      for job in streamJobs:
         result = WSEL_step4(job)
      #pool=Pool(processes=proc)
      #result = pool.map(WSEL_step4,streamJobs)
      #pool.close()
      #pool.join()

   else:
      print("Beginning Step 4")
      WSEL_step4(streamJobs[0])
   print("Step 4 completed")
   if setup['backwater']!= True:
      print_to_log(setup,"Step 4","Complete")
      print_to_config(setup,"Step4",True)
   return

def Step5(setup,proc,streamJobs,multi):
   #For some reason tin creation will not work with multiprocessing 
   multiproc =  multi
   if multiproc == True:
      print("Beginning Step 5 module")
      for job in streamJobs:
         result = WSEL_step5(job)
      #pool=Pool(processes=proc)
      #result = pool.map(WSEL_step5,streamJobs)
      #pool.close()
      #pool.join()
   else:
      print("Beginning Step 5")
      WSEL_step5(streamJobs[0])
   print("Step 5 completed")
   print_to_log(setup,"Step 5","Complete")
   print_to_config(setup,"Step5",True)
   return


def main(config):
   setup = config['Setup']
   logging.basicConfig(level=logging.DEBUG, filename=setup['logfile'], format='%(asctime)s %(message)s')
   try:
      print_to_log(setup,"Start Time", time.strftime("%c"))

      multi = setup['multiproc']
      processors = setup['Proc']

      if 'Proc' not in config:
         config['Proc'] = processors
      else:
         proc = config['Proc']

      if 'StreamCount' not in config:
         stream_count = 0
      else:
         stream_count = config['StreamCount']
      if 'JobConfig_1' in config:
         job_config_1 = config['JobConfig_1']
      else:
         config['JobConfig_1'] = ''

      if 'JobConfig' in config:
         job_config = config['JobConfig']
      else:
         config['JobConfig'] = ''

      if 'StreamJobs' in config:
         streamJobs = config['StreamJobs']
      else:
         config['StreamJobs'] = False

      if 'OriginalWorkspace' not in config:
         config['OriginalWorkspace'] = False
      if 'CopyWorkspace' not in config:
         config['CopyWorkspace'] = False
      if 'ScratchWorkspace' not in config:
         config['ScratchWorkspace'] = False
      if 'StreamSetup' not in config:
         config['StreamSetup'] = False
      if 'Intersects' not in config:
         config['Intersects'] = False
      if 'IntersectsClean' not in config:
         config['IntersectsClean'] = False

      if 'Step4' not in config:
         config['Step4'] = False
      if 'Step5' not in config:
         config['Step5'] = False
      if 'XSCheck' not in config:
         config['XSCheck'] = False
      if 'MergeStreams_1' not in config:
         config['MergeStreams_1'] = False

      if 'MergeIntersects_1' not in config:
         config['MergeIntersects_1'] = False

      if 'IntersectsDelete' not in config:
         config['IntersectsDelete'] = False
      if 'finalize_data' not in config:
         config['finalize_data'] = False
      if 'comb_raster' not in config:
         config['comb_raster'] = False
      if 'stream_list' in config:
         stream_list = config['stream_list']         
      else:
         config['stream_list'] = ''
         stream_list = config['stream_list']

      if config['OriginalWorkspace']== False:
         OriginalWorkspace(setup)
         print_to_log(setup,"Multiproc",multi)
      if config['CopyWorkspace']==False:
         stream_count,job_config_1=CopyWorkspace(setup)
      
      if config['ScratchWorkspace']== False :
         proc, job_config =ScratchWorkspace(setup,stream_count, job_config_1, processors)
      if config['StreamJobs']==False:
         streamJobs = StreamJobs(setup,job_config,proc)
         i=1
         for job in streamJobs:
            print_to_log(setup,"Processor "+ str(i),json.dumps(job['stream_names']))
            i=i+1
      
      if config['StreamSetup']== False:
         StreamSetup(setup,proc,streamJobs)
      if config['XSCheck']== False:
         XSCheck(setup,proc,streamJobs)
      
      if setup['backwater']== True:
         if config['MergeStreams_1'] == False:
            MergeStreams(setup,streamJobs,proc,1, multi)
         if config['Intersects']== False:
            IntersectJob(setup,proc,streamJobs)
         if config['MergeIntersects_1'] == False:
            MergeIntersects(setup,streamJobs,proc, 1, multi)
         if config['IntersectsClean'] == False:
            IntersectsClean(setup,proc,streamJobs)
         if config['stream_list'] == '':
            stream_list=stream_order(setup,streamJobs)
                   

         list_length = len(stream_list)
         i=0
         if config['IntersectsDelete'] ==False:
            Intersects_delete(setup,streamJobs,proc)
         
         completed_streams =[]

         for streamname in stream_list:
            if streamname not in config:
               print("Processing "+streamname+" "+str(i)+" out of "+str(list_length)+" streams completed")
               completed_streams.append(streamname)
               stream = streamname
               if multi != False:
                  loc = getConfig(stream,streamJobs,proc)
               else:
                  loc = 0
               print(multi)
               stream_config = streamJobs[loc]['config']
               single_config =[{'stream_names':[stream],'config':stream_config, 'completed': completed_streams}]
               single_config[0]['config']['multiproc']=False
               Step1(setup,proc,single_config)
               MergeStreams(setup,streamJobs,proc,2,multi)
               Step2(setup,proc,single_config)
               MergeIntersects(setup,streamJobs,proc,2,multi)
               Step3(setup,proc,single_config)
               Step4(setup,proc,single_config, False)
               
               print_to_config(setup,streamname,True)
            i=i+1
      else:
         if config['Step4'] == False:
            Step4(setup,proc,streamJobs, True)
      return
      if config['Step5'] == False:
         Step5(setup,proc,streamJobs,multi)
      
      
      if config['finalize_data']== False:
         finalize_data(setup,streamJobs,proc,multi)
      if config['comb_raster'] ==False:
         comb_raster(setup,streamJobs,proc,multi)

      print_to_log(setup,"End Time", time.strftime("%c"))
   except:
      logging.exception(" Error:")



class LicenseError(Exception):
   def __init__(self, arg):
      self.msg = arg

if __name__ == "__main__":
   try:
      if arcpy.CheckExtension("3D") == "Available":
         print("3D extension available")
         arcpy.CheckOutExtension("3D")
      else:
         raise LicenseError("3D")
      if arcpy.CheckExtension("Spatial") == "Available":
         print("Spatial extension available")
         arcpy.CheckOutExtension("Spatial")
      else:
         raise LicenseError("Spatial Analyst")

      runs =["WSE"]
      sl = os.path.abspath(os.path.dirname(sys.argv[0]))

      for r in runs:
         print("Running script for "+r)
         configfile =''
         setup = Script_setup(False,sl, r)
         configfile = setup['configfile']
         if not os.path.exists(configfile):
            config ={'Setup':setup}
         else:
            config={'Setup': setup}
            with open(configfile, mode='r') as f:
               while 1:
                  try:
                     config.update(cPickle.load(f))
                  except EOFError:
                     break
         main(config)
         

   except LicenseError, arg:
      print(arg.msg+" license is not available.")

   finally:
      # Check in all extensions
      arcpy.CheckInExtension("3D")
      arcpy.CheckInExtension("Spatial")
