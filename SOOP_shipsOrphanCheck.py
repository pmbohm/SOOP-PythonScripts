#!/bin/env python
# -*- coding: utf-8 -*-
import sys,threading
import FTPGetter

if __name__ == "__main__":
    

    getter = FTPGetter.FTPGetter()
    
    try:
        pass       
        #   def checkLocalDataset(self, url, localDir, filetype , username, password, getsubdirs, useDatafabric):
        getter.checkLocalDataset("ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/SHIPS", "/opt/SOOP_cache/ships/",'','bom404','Vee8soxo', True, False)
        #getter.checkLocalDataset("ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/MOORINGS",  "opendap/ABOS/ASFS/SOFS",'','bom404','Vee8soxo', True,  True)
    except:
        print "ERROR: uncaught error occured with ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/SHIPS"
        pass

    
    getter.close()
    

	

   

