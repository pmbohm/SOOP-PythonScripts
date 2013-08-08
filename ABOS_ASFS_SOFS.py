#!/bin/env python
# -*- coding: utf-8 -*-
import sys,threading
import FTPGetter, StorageConnection

if __name__ == "__main__":


   
    dfNeeded = True # needed if using the Datafabric
    df = StorageConnection.StorageConnection()

    if dfNeeded:
        
        if df.connectDatafabric():

            
            getter = FTPGetter.FTPGetter()
            local_dir = "/mnt"
            
                
            try:
                pass #marty is replacing this with a script to sort as well
                #getter.processDataset("ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/MOORINGS", local_dir +  "/opendap/ABOS/ASFS/SOFS",'','bom404','Vee8soxo', True, True, False)
            except:
                pass

            
            getter.close()

      
    
