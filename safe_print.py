from __future__ import print_function
import arcpy

class Safe_Print:

    def __init__(self, config):
        self.config = config

    def __enter__(self):
        self.multi=self.config['multiproc']
