#!/bin/env python
# -*- coding: utf-8 -*-
import sys, threading
import FTPGetter,soop_realtime_processSBD, StorageConnection

if __name__ == "__main__":


    getter = FTPGetter.FTPGetter()    
    local_dir = "/var/lib/SOOP_cache/"
    filesChanged = False;
    
    try:
        pass
        filesChanged = getter.processDataset("ftp://ftp.marine.csiro.au/pub/gronell/SBDdata", local_dir + "sbddata",'sbd','','', True, False, True)
    except Exception, e:
        msg = "Failed to retrieve files "
        print (msg + " " + str(e))

    getter.close()
    
    # testing only
    filesChanged = True;

    if filesChanged:
      
        df = StorageConnection.StorageConnection()        
        if df.connectStorage():

            processSBD = soop_realtime_processSBD.soop_realtime_processSBD()
            #try:
            #pass
            processSBD.processFiles( local_dir + "sbddata")
            #except:
            #    pass   


      
    
