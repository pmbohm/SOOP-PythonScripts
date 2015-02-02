#!/bin/env python
# -*- coding: utf-8 -*-
import sys,threading
import FTPGetter, soop_FileSort

if __name__ == "__main__":

    
    getter = FTPGetter.FTPGetter()
    new_local_dir = "/opt/SOOP_cache/"
    
    filesChanged = False
    
    
    
    try:
        
        filesChanged =  getter.processDataset("ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/SHIPS", new_local_dir + "ships",'','[add]','[add]', True, False, True)
    except Exception, e:
        print "ERROR: uncaught error occured with ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/SHIPS" + str(e)
        pass
    


    
    getter.close()

    if filesChanged:
      filesort = soop_FileSort.soop_FileSort()
      try:
        #pass
        filesort.processFiles(True, new_local_dir + "ships/","","nc")
      except Exception, e:
        print "ERROR: uncaught error occured with FileSort " +new_local_dir +  "ships/" + str(e)
        pass
      filesort.close()
      

   

