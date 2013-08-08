#!/usr/ bin/env python
# -*- coding: utf-8 -*-
import os, sys, time, threading
from subprocess import Popen, PIPE, STDOUT


class StorageConnection:


    def __init__(self):
        """        
            This file used to manage connection to the Storage.
            This file exists previously called DatafabricConnection.py to handle mounting and unmouting the datafabric
            Now storage connections are reliably auto-mounted   
                    
        """

        self.storageDir = "/mnt/imos-t3" 
        

    def connectStorage(self):
        return self.isStorageMounted()


    def unconnectStorage(self):
        """
            Leave it up to automounter to disconnect
        """
        
        return True

    def isStorageMounted(self):
        """
            # stat on the directory to trigger automount
            # Then check the output of mount
        """
        #subprocess.call('ls  ' + self.storageDir)
        #os.system('mount ' + self.storageDir) # call mount directly and wait

        p = Popen("mount | grep " + self.storageDir , shell=True, bufsize=0,  stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        (stdout,stderr) = p.communicate()                 
        if stdout.find(self.storageDir) != -1:
            print "It seems the Automount is fine"
            return True
        else: 
            print "It seems the Automount isnt working. Contact the sys admin"
            return False
                

        
        
            

if __name__ == "__main__":
    
    d = StorageConnection()
    d.connectStorage()
