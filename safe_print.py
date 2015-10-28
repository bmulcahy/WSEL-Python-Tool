from __future__ import print_function
import arcpy

class Safe_Print:
    def __init__(self, config):
        self.multi= config['multi']
        self.modelbuilder=config['modelbuilder']
        

    def print_out(self,text):
        if self.multi == False and self.modelbuilder == False:
            print(text)
        if self.multi == False and self.modelbuilder == True:
            arcpy.AddMessage(text)
