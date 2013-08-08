#!/bin/env python
# -*- coding: utf-8 -*-
import os



class cleaningNC:

    def __init__(self):
      """
      cleanout *.nc files from folders in supplied parent folder
      The first level of folders are to be checked ignoreing sub folders called profiles
      
      """

      self.cleanedCount = 0;
      self.status = []
      self.errorFiles = []

    def processFiles(self,origDir):
        
     
      self.sbddataFolder = origDir
      
      os.chdir(origDir)
      for root, dirs, fname in os.walk(os.getcwd()):
	  fname.sort()
	  for fname in fname:
	      if fname.rsplit(".",1)[1] == "nc":
		  # ignore profiles directories
		  if (root[-9:] != "/profiles"):
		    self.handleFiles(root,fname)      
          
    
    def handleFiles(self,root, fname):            
      
      theFile = root+"/"+fname
      
      try:
	os.remove(theFile)
      except Exception, e:
	print "ERROR: Failed to remove " + theFile
	
      try:
	open(theFile)
        print "failed to remove " + theFile
      except Exception, e:      
	print "Removed: " + theFile          
      
     


if __name__ == "__main__":
        f = cleaningNC()
        #f.processFiles("/home/smancini/datafabric_root/opendap/AATAMS/sattag")
