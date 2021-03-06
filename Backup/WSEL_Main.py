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



def Script_setup(check):
   setup={}
   if check==True:
      #TODO add support for arcgis toolbox
      scriptlocation = arcpy.GetParameterAsText(0)
      multiproc = arcpy.GetParameterAsText(1)
      proc = arcpy.GetParameterAsText(2)
      projectname = arcpy.GetParameterAsText(3)
      rootdir = arcpy.GetParameterAsText(4)
      sr=arcpy.GetParameterAsText(5)
   else:
      scriptlocation = os.path.abspath(os.path.dirname(sys.argv[0]))
      multiproc = False
      proc = 4
      projectname = "LowerCottonwood"
      rootdir = "C:/Users/bmulcahy/External/Projects/Tools/wsel/script/debug jacobCreek"
      sr="WGS 1984 UTM Zone 14N"
   
   scratch = os.path.join(scriptlocation,"full")
   if not os.path.exists(scratch):
      os.makedirs(scratch)
   configfile = os.path.join(scratch,"config.cfg")
   logfile = os.path.join(scratch,"log.txt")
   originalgdb = os.path.join(scratch,"Original.gdb")
   streams_original = os.path.join(originalgdb,'streams')
   flood_original = os.path.join(originalgdb,'flood')
   xs_original = os.path.join(originalgdb,'xs')
   final = os.path.join(scratch,"Output")   
   comb = os.path.join(scratch,"combined_output")
   combinedgdb = os.path.join(comb,"Comb.gdb")
   combined_workspace = os.path.join(comb,"Comb.gdb\\")
   comb_rast = os.path.join(combinedgdb,projectname)
   setup ={'sr': sr,'Rootdir':rootdir,'Projectname': projectname,'Multiproc': multiproc,'Proc': proc,'scratch': scratch,'logfile': logfile,'configfile': configfile,'originalgdb': originalgdb,'streams_original': streams_original,'flood_original':flood_original,'xs_original':xs_original,'final':final,'comb':comb,'combinedgdb':combinedgdb,'combined_workspace':combined_workspace,'comb_rast':comb_rast}
   
   return setup


def print_to_log(setup, param, data):
   logfile = setup['logfile']   
   with open(logfile, "ab+") as text_file:
      print("{} = {}".format(param,data), end='\n',file=text_file)

def print_to_config(setup, param, data):
   configfile = setup['configfile']   
   topickle = {param: data}   
   with open(configfile, "a+b") as text_file:
      cPickle.dump(topickle,text_file)

def OriginalWorkspace(setup):
   print("Creating workspace")
   scratch = setup['scratch']
   final= setup['final']
   comb= setup['comb']
   combinedgdb= setup['combinedgdb']
   originalgdb= setup['originalgdb']
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   flood_original= setup['flood_original']
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
   if not os.path.exists(originalgdb):
      arcpy.CreateFileGDB_management(scratch, "Original.gdb")
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

def CopyWorkspace(setup):
   print("Copying data into workspace")  
   
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   streams_dataset= setup['streams_original']
   flood_original= setup['flood_original']
   rootdir = setup['Rootdir']
   job_config ={'config':[],'stream_names':[]}

   
   incomplete_streams =[]
   stream_count = 0
   for name in os.listdir(rootdir):
      if os.path.isdir(os.path.join(rootdir, name)):
         directory = os.path.join(rootdir, name)
         stream_loc = os.path.join(directory, name+'.shp')
         xs_original_loc = os.path.join(directory, name+'_xsecs_results.shp')
         boundary_loc = os.path.join(directory, name+'_flood.shp')
         if os.path.isfile(stream_loc) and os.path.isfile(xs_original_loc) and os.path.isfile(boundary_loc):
            print("Copying "+name+" data into file geodatabase")
            stream_count=stream_count+1
            job_config['stream_names'].append(name+"_stream_feature")
            arcpy.FeatureToLine_management(xs_original_loc, xs_original+"/"+name+"_xs")            
            arcpy.FeatureToLine_management(stream_loc,streams_original+"/"+name+"_stream_feature")            
            arcpy.FeatureToPolygon_management(boundary_loc,flood_original+"/"+name+"_flood_boundary")
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

def ScratchWorkspace(setup, multi,stream_count, job_config, proc):
   scratch = setup['scratch']
   final= setup['final']
   comb= setup['comb']
   combinedgdb= setup['combinedgdb']
   originalgdb= setup['originalgdb']
   xs_original= setup['xs_original']
   streams_original= setup['streams_original']
   flood_original= setup['flood_original']
   comb_rast= setup['comb_rast']
   configfile = setup['configfile']   
   sr = setup['sr']
   projectname = setup['Projectname']
   
   if stream_count < proc:
      proc = stream_count
   if multi == True:      
      for p in range(proc):
         i = p+1
         print("Creating workspace for processor "+str(i))
         scratchoriginal = scratch         
         scratchproc = os.path.join(scratchoriginal,"proc"+str(i))         
         scratchgdb = os.path.join(scratchproc,"Scratch.gdb")
         finalgdb = os.path.join(scratchproc,"Final.gdb")
         tablefolder = os.path.join(scratchproc,"Tables")
         output_workspace = os.path.join(scratchproc,"Final.gdb\\")
         raster_catalog = os.path.join(finalgdb,projectname)         
         streams_dataset = os.path.join(scratchgdb,'streams')
         streams_intersect_dataset = os.path.join(scratchgdb,'streams_intersect')
         xs_intersect_dataset = os.path.join(scratchgdb,'xs_intersect')
         vertices_dataset = os.path.join(scratchgdb,'vertices')
         routes_dataset = os.path.join(scratchgdb,'routes')
         xs_dataset = os.path.join(scratchgdb,'xs')
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
            arcpy.CreateFileGDB_management(scratchproc, "Final.gdb")         
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
         if not arcpy.Exists(streams_zm):
            arcpy.CreateFeatureDataset_management(scratchgdb, "streams_zm", sr)
         if not arcpy.Exists(raster_catalog):
            arcpy.CreateRasterCatalog_management(finalgdb, projectname)
         else:
            arcpy.DeleteRasterCatalogItems_management(raster_catalog)
         
         job_config['config'].append({'table_folder':tablefolder,'tin_folder':tin_folder,'configfile': configfile,'streams_zm':streams_zm, 'scratch': scratchproc,'sr': sr,'originalgdb': originalgdb, 'scratchgdb':scratchgdb,'finalgdb':finalgdb,'output_workspace':output_workspace,'raster_catalog':raster_catalog,'streams_dataset':streams_dataset,'streams_intersect_dataset':streams_intersect_dataset,'xs_intersect_dataset':xs_intersect_dataset,'vertices_dataset':vertices_dataset,'routes_dataset':routes_dataset,'xs_dataset':xs_dataset,'streams_original':streams_original,'xs_original':xs_original,'flood_original':flood_original})
         
   else:
      scratchgdb = os.path.join(scratch,"Scratch.gdb")
      finalgdb = os.path.join(scratch,"Final.gdb")
      tablefolder = os.path.join(scratch,"Tables")
      output_workspace = os.path.join(scratch,"Final.gdb\\")
      raster_catalog = os.path.join(finalgdb,projectname)
      streams_dataset = os.path.join(scratchgdb,'streams')
      streams_intersect_dataset = os.path.join(scratchgdb,'streams_intersect')
      xs_intersect_dataset = os.path.join(scratchgdb,'xs_intersect')
      vertices_dataset = os.path.join(scratchgdb,'vertices')
      routes_dataset = os.path.join(scratchgdb,'routes')
      xs_dataset = os.path.join(scratchgdb,'xs')
      streams_zm = os.path.join(scratchgdb,'streams_zm')
      tin_folder = os.path.join(scratch,"Tins")
      if not os.path.exists(tin_folder):
         os.makedirs(tin_folder)
      if not os.path.exists(scratchgdb):         
         arcpy.CreateFileGDB_management(scratch, "Scratch.gdb")
      if not os.path.exists(finalgdb):
         arcpy.CreateFileGDB_management(scratch, "Final.gdb")
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
      if not arcpy.Exists(streams_zm):
         arcpy.CreateFeatureDataset_management(scratchgdb, "streams_zm", sr)
      if not os.path.exists(raster_catalog):
         arcpy.CreateRasterCatalog_management(finalgdb, projectname)
      else:
         arcpy.DeleteRasterCatalogItems_management(raster_catalog)
      job_config['config'].append({'table_folder':tablefolder,'tin_folder':tin_folder,'configfile': configfile,'streams_zm':streams_zm,'scratch':scratch,'sr':sr, 'originalgdb': originalgdb, 'scratchgdb':scratchgdb,'finalgdb':finalgdb,'output_workspace':output_workspace,'raster_catalog':raster_catalog,'streams_dataset':streams_dataset,'streams_intersect_dataset':streams_intersect_dataset,'xs_intersect_dataset':xs_intersect_dataset,'vertices_dataset':vertices_dataset,'routes_dataset':routes_dataset,'xs_dataset':xs_dataset,'streams_original':streams_original,'xs_original':xs_original,'flood_original':flood_original})
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
      if not result:
         return
      else:
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
    with WSEL_Step_2(config) as wsel_Step_2:
       wsel_Step_2.processStream()
       return

def WSEL_step3(streamJobs):    
    config=streamJobs['config']
    with WSEL_Step_3(config) as wsel_Step_3:
       result=wsel_Step_3.processStream()
       if not result:
          return
       else:
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
   


def StreamJobs(config,multi,procs):
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

def MergeStreams(setup,job_config,multi, proc, run):
   print("Merging Streams")
   multiproc = multi
   stream_list =[]
   
   if multiproc == True:
      for p in range(proc):
         if run == 1:
            stream_loc = job_config['config'][p]['streams_dataset']
         else:
            stream_loc = job_config['config'][p]['routes_dataset']
         scratchgdb_loc = job_config['config'][p]['scratchgdb']
         env.workspace = stream_loc
         env.overwriteOutput = True
         streams = arcpy.ListFeatureClasses()
         env.workspace = scratchgdb_loc
         local_streams = arcpy.Merge_management(streams, "local_streams_all")
         stream_list.append(scratchgdb_loc+'\\local_streams_all')
      for p in range(proc):
         scratchgdb_loc = job_config['config'][p]['scratchgdb']
         env.workspace = scratchgdb_loc
         env.overwriteOutput = True
         comb_streams = arcpy.Merge_management(stream_list, "streams_all")
   else:
      if run == 1:
        stream_loc = job_config['config'][0]['streams_dataset']
      else:
         stream_loc = job_config['config'][0]['routes_dataset']
      scratchgdb_loc = job_config['config'][0]['scratchgdb']
      env.workspace = stream_loc
      env.overwriteOutput = True
      streams = arcpy.ListFeatureClasses()
      env.workspace = scratchgdb_loc
      env.overwriteOutput = True
      comb_streams = arcpy.Merge_management(streams, "streams_all")     
   print_to_log(setup,"MergeStreams_"+str(run),"Complete")
   print_to_config(setup,"MergeStreams_"+str(run),True)
   print("Merge Streams completed")
   return comb_streams

def MergeIntersects(setup,job_config,multi,proc,run):
   stream_intersect =[]
   multiproc = multi
   run=run
   print("Merging Stream Intersects")
   if multiproc == True:
      for p in range(proc):         
         stream_loc = job_config['config'][p]['streams_intersect_dataset']
         scratchgdb_loc = job_config['config'][p]['scratchgdb']
         env.workspace = stream_loc
         streams = arcpy.ListFeatureClasses()
         env.workspace = scratchgdb_loc
         local_streams = arcpy.Merge_management(streams, "local_streams_intersect_all_"+str(run))
         stream_intersect.append(scratchgdb_loc+'\\local_streams_intersect_all_'+str(run))
      for p in range(proc):
         scratchgdb_loc = job_config['config'][p]['scratchgdb']
         env.workspace = scratchgdb_loc
         comb_intersect = arcpy.Merge_management(stream_intersect, "streams_intersect_all_"+str(run))
   else:
      stream_loc = job_config['config'][0]['streams_intersect_dataset']
      scratchgdb_loc = job_config['config'][0]['scratchgdb']
      env.workspace = stream_loc
      streams = arcpy.ListFeatureClasses()
      env.workspace = scratchgdb_loc
      comb_intersect = arcpy.Merge_management(streams, "streams_intersect_all_"+str(run))    
   print_to_log(setup,"MergeIntersects_"+str(run),"Complete")
   print_to_config(setup,"MergeIntersects_"+str(run),True)
   print("Merge Intersects completed")
   return comb_intersect

def comb_raster(setup,multi,job_config,proc):
   comb_rast =setup['comb_rast']
   final = setup['final']
   projectname = setup['Projectname']
   print("Combining rasters")
   if multi == True:
      for p in range(proc):         
         raster_loc = job_config['config'][p]['output_workspace']
         raster_cat = comb_rast
         arcpy.WorkspaceToRasterCatalog_management(raster_loc, raster_cat,"INCLUDE_SUBDIRECTORIES","PROJECT_ONFLY")
      arcpy.RasterCatalogToRasterDataset_management(raster_cat,os.path.join(final,projectname+".tif"),"", "MAXIMUM", "FIRST","", "", "32_BIT_FLOAT")
   else:
      raster_loc = job_config['config'][0]['output_workspace']
      raster_cat = job_config['config'][0]['raster_catalog']
      arcpy.WorkspaceToRasterCatalog_management(raster_loc, raster_cat,"INCLUDE_SUBDIRECTORIES","PROJECT_ONFLY")
      arcpy.RasterCatalogToRasterDataset_management(raster_cat,os.path.join(final,"LowerCottonwood.tif"),"", "MAXIMUM", "FIRST","", "", "32_BIT_FLOAT")
   print("All water surface elevations rasters have been created")

def XSCheck(setup,proc,multi,streamJobs):
   warning ={}
   error = 0
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
         warning.update(result[0])
   
   print("XS have been checked")
   if error >0:
      warning_string=json.dumps(warning)
      print_to_log(setup,"XSWarnings",warning_string)
   print_to_log(setup,"XSCheck","Complete")
   print_to_config(setup,"XSCheck",True)
   
def StreamSetup(setup,proc,multi,streamJobs):
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

def IntersectJob(setup,proc,multi,streamJobs):
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

def IntersectsClean(setup,proc,multi,streamJobs):
   if multi == True:
      print("Beginning Intersects Clean using multiprocesser module")   
      for job in streamJobs:
         result = WSEL_IntersectsClean(job)
      #pool=Pool(processes=proc)      
      #result = pool.map(WSEL_IntersectsClean,streamJobs)
      #pool.close()
      #pool.join()
   else:      
      print("Beginning Intersects Clean without multiprocesser module")
      WSEL_IntersectsClean(streamJobs[0])   
   print("Stream Intersects Clean completed")
   print_to_log(setup,"IntersectsClean","Complete")
   print_to_config(setup,"IntersectsClean",True)
   
def Step1(setup,proc,multi,streamJobs):
   if multi == True:          
      print("Beginning Step 1 using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_step1(job)
      pool=Pool(processes=proc) 
      result = pool.map(WSEL_step1,streamJobs)
      pool.close()
      pool.join()      
   else:
      print("Beginning Step 1 without multiprocesser module")
      WSEL_step1(streamJobs[0])
   print("Step 1 completed")
   print("Streams have been configured")
   print_to_log(setup,"Step 1","Complete")
   print_to_config(setup,"Step1",True)

def Step2(setup,proc,multi,streamJobs):
   if multi == True:
      print("Beginning Step 2 using multiprocesser module")   
      #for job in streamJobs:
         #result = WSEL_step2(job)
      pool=Pool(processes=proc)      
      result = pool.map(WSEL_step2,streamJobs)
      pool.close()
      pool.join()
   else:      
      print("Beginning Step 2 without multiprocesser module")
      WSEL_step2(streamJobs[0])   
   print("Step 2 completed")
   print_to_log(setup,"Step 2","Complete")
   print_to_config(setup,"Step2",True)

def Step3(setup,proc,multi,streamJobs):
   error = 0
   warning ={}
   if multi == True:
      print("Beginning Step 3 using multiprocesser module")
      #for job in streamJobs:
         #result = WSEL_step3(job)
         #if result != None:
            #warning.update(result[0])         
      pool=Pool(processes=proc)
      result = pool.map(WSEL_step3,streamJobs)
      pool.close()
      pool.join()
      print_to_log(setup,"Step 3","Complete")
      print_to_config(setup,"Step3",True)
      #if result!=None:         
         #for i in result:
            #if i[0] != None:               
               #warning.update(i[0])
   else:      
      print("Beginning Step 3 without multiprocesser module")
      result=WSEL_step3(streamJobs[0])
      print_to_log(setup,"Step 3","Complete")
      print_to_config(setup,"Step3",True)
      #if result != None:
         #warning.update(result[0])
   if error >0:
      warning_string=json.dumps(warning)   
      print_to_log(setup,"Stream Intersect Warning",warning_string)
   print("Step 3 completed")   
   

def Step4(setup,proc,multi,streamJobs):
   if multi == True:
      print("Beginning Step 4")   
      for job in streamJobs:
         result = WSEL_step4(job)         
      #pool=Pool(processes=proc)     
      #result = pool.map(WSEL_step4,streamJobs)
      #pool.close()
      #pool.join()
      
   else:      
      print("Beginning Step 4 without multiprocesser module")
      WSEL_step4(streamJobs[0])
   print("Step 4 completed")
   print_to_log(setup,"Step 4","Complete")
   print_to_config(setup,"Step4",True)

def Step5(setup,proc,multi,streamJobs):
   if multi == True:      
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
   
   
def main(config):          
   setup = config['Setup']
   logging.basicConfig(level=logging.DEBUG, filename=setup['logfile'], format='%(asctime)s %(message)s')
   try:
      print_to_log(setup,"Start Time", time.strftime("%c"))
      
      multi = setup['Multiproc']   
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
      if 'Step1' not in config:
         config['Step1'] = False
      if 'Step2' not in config:
         config['Step2'] = False
      if 'Step3' not in config:
         config['Step3'] = False
      if 'Step4' not in config:
         config['Step4'] = False
      if 'Step5' not in config:
         config['Step5'] = False
      if 'XSCheck' not in config:
         config['XSCheck'] = False
      if 'MergeStreams_1' not in config:
         config['MergeStreams_1'] = False
      if 'MergeStreams_2' not in config:
         config['MergeStreams_2'] = False
      if 'MergeIntersects_1' not in config:
         config['MergeIntersects_1'] = False
      if 'MergeIntersects_2' not in config:
         config['MergeIntersects_2'] = False
      if config['OriginalWorkspace']== False:
         OriginalWorkspace(setup)
         print_to_log(setup,"Multiproc",multi)      
      if config['CopyWorkspace']==False:
         stream_count,job_config_1=CopyWorkspace(setup)      
      if config['ScratchWorkspace']== False :
         proc, job_config =ScratchWorkspace(setup,multi,stream_count, job_config_1, processors)      
      if config['StreamJobs']==False:      
         streamJobs = StreamJobs(job_config,multi, proc)
         i=1
         for job in streamJobs:
            print_to_log(setup,"Processor "+ str(i),json.dumps(job['stream_names']))
            i=i+1      
      if config['StreamSetup']== False:
         StreamSetup(setup,proc,multi,streamJobs)
      if config['MergeStreams_1'] == False:
         MergeStreams(setup,job_config,multi,proc,1)      
      if config['Intersects']== False:
         IntersectJob(setup,proc,multi,streamJobs)      
      if config['MergeIntersects_1'] == False:
         MergeIntersects(setup,job_config,multi,proc, 1)           
      if config['IntersectsClean'] == False:
         IntersectsClean(setup,proc,multi,streamJobs)
      return
      if config['XSCheck']== False:
         XSCheck(setup,proc,multi,streamJobs)      
      if config['Step1'] == False:
         Step1(setup,proc,multi,streamJobs)      
      if config['MergeStreams_2'] == False:
         MergeStreams(setup,job_config,multi,proc,2)      
      if config['Step2'] == False:
         Step2(setup,proc,multi,streamJobs)      
      if config['MergeIntersects_2'] == False:
         MergeIntersects(setup,job_config,multi,proc,2)
      
      if config['Step3'] == False:
         Step3(setup,proc,multi,streamJobs)
         
         
      return
      if config['Step4'] == False:
         Step4(setup,proc,multi,streamJobs)      
      if config['Step5'] == False:
         Step5(setup,proc,multi,streamJobs)   
   
      comb_raster(setup,multi,job_config,proc)
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

      setup = Script_setup(False)
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
