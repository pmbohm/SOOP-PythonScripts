#!/bin/env python
# -*- coding: utf-8 -*-
import os, shutil, stat, time, grp, sys, threading
from subprocess import Popen, PIPE, STDOUT
import StorageConnection
import sendEmail


class soop_FileSort:

    def __init__(self):

        # run as sudo -u cron_scripts -bash

        self.ships = {'VLST':'Spirit-of-Tasmania-1',
                                'VNSZ':'Spirit-of-Tasmania-2',
                                'VHW5167':'Sea-Flyte',
                                'VNVR':'Iron-Yandi',
                                'VJQ7467':'Fantasea',
                                'VNAH':'Portland',
                                'V2BJ5':'ANL-Yarrunga',
                                'VROB':'Highland-Chief',
                                '9HA2479':'Pacific-Sun',
                                'VNAA':'Aurora-Australis',
                                'VLHJ':'Southern-Surveyor',
                                'FHZI':'Astrolabe',
                                'C6FS9':'Stadacona',
                                'V2BF1':'Florence',
                                'V2BP4':'Vega-Gotland',
                                'A8SW3':'Buxlink',
                                'ZMFR':'Tangaroa',
                                'VRZN9': 'Pacific-Celebes',
                                'VHW6005':'RV-Linnaeus',
                                'HSB3402':'MV-Xutra-Bhum',
                                'HSB3403':'MV-Wana-Bhum',
                                'VRUB2': 'Chenan'
                                }

        self.data_codes = {'FMT':'flux_product','MT':'meteorological_sst_observations' }
        
        self.datasetGroup = "users" # the linux group and user must exist
        # place where sorted files go
        self.dataRoot = "/mnt/imos-t3/IMOS"
        self.destDir = self.dataRoot  + "/opendap/SOOP" # temp value only if not supplied

        # place for log files. Same as FTPGetter.py
        self.localBaseDir = "/home/pmbohm/script_logs/"
        


        # Use this to convert target modified time into local time
        self.timezone_offset = time.altzone

        self.newCount = 0;
        self.updatedCount = 0;
        self.checkedCount = 0;
        self.ignoredCount = 0;

        self.status = []
        self.errorFiles = []

    def processFiles(self,useDataFabric,origDir,userDestDir,fileExtension):
        
        if len(userDestDir) > 0:
            self.destDir = userDestDir
            
        if useDataFabric:
        
            df = StorageConnection.StorageConnection()          
            if df.isStorageMounted():
                print "Connected to the storage"
                self.status.append("Connected to the storage")
            else:
                df.connectStorage()
            if not df.isStorageMounted():   
                print "Failed to connect to the storage so exiting."
                self.errorFiles.append("Failed to connect to storage")
                sys.exit()

        os.chdir(origDir)
        print "Checking " + os.getcwd()
        for root, dirs, fname in os.walk(os.getcwd()):
            fname.sort()
            for fname in fname:
                if fname.rsplit(".",1)[1] == fileExtension:
                    modified = os.stat(root+"/"+fname)[stat.ST_MTIME]
                    #print str(time.localtime(modified)) + " " + root+"/"+fname
                    self.handleFiles(fname,modified,root)
                    
                else:
                    self.ignoredCount += 1
        

        os.system("find " +  origDir + " -name '*." + fileExtension +"' -size -5b -exec rm {\} \;")
        # find crap files created on the destination 
        crapFiles = os.system("find " +  self.destDir + " -name '*." + fileExtension +"' -size -5b -exec ls -la {\} \;")
        if crapFiles > 0:
            os.system("find " +  self.destDir + " -name '*." + fileExtension +"' -size -5b -exec rm {\} \;")
            self.errorFiles.append(str(crapFiles) + " empty ." +fileExtension +" files removed")

        self.status.append("\nSummary for File Sort: " + origDir + " " + time.strftime("%a, %d %b %Y %H:%M:%S",time.localtime()) )
        self.status.append(str(self.updatedCount) + " Files were updated: ")
        self.status.append(str(self.newCount) + " New files: ")
        self.status.append(str(len(self.errorFiles)) + " Problems: ")
        for f in self.errorFiles:
            self.status.append(f)
        self.status.append(str(self.checkedCount)+" files up to date")
        self.status.append("==============================")

        self.writetoLog(self.status)
        # print to console
        for f in self.status:
            print f

    def handleFiles(self,fname,modified,root):
        """
        
        # eg file IMOS_SOOP-SST_T_20081230T000900Z_VHW5167_FV01.nc
        # IMOS_<Facility-Code>_<Data-Code>_<Start-date>_<Platform-Code>_FV<File-Version>_<Product-Type>_END-<End-date>_C-<Creation_date>_<PARTX>.nc
        
        """
        
        theFile = root+"/"+fname

        
        file = fname.split("_")
        #print theFile

        if file[0] != "IMOS":
            self.ignoredCount += 1
            return

        facility = file[1] # <Facility-Code>
        
        
        # the file name must have at least 6 component parts to be valid
        if len(file) > 5:
            
            year = file[3][:4] # year out of <Start-date>
            

            # check for the code in the ships
            code = file[4]
            if code in self.ships:

                platform = code+"_"+ self.ships[code]

                if facility == "SOOP-ASF":
                    if file[2] in self.data_codes:
                        product = self.data_codes[file[2]]
                        targetDir = self.destDir+"/"+facility+"/"+platform+"/"+product+"/"+year
                        targetDirBase = self.destDir+"/"+facility
                    else:
                        err = "Unknown Data Code "+product+" for "+facilty+". Add it to this script. File ignored"
                        print err
                        self.errorFiles.append(err)
                        # common error that needs  our attention
                        email = sendEmail.sendEmail()
                        email.sendEmail("pmbohm@utas.edu.au","SOOP Filesorter - Unrecognised Data code",err)
                        self.ignoredCount += 1
                        return False
                else:
                    targetDir = self.destDir+"/"+facility+"/"+platform+"/"+year
                    targetDirBase = self.destDir+"/"+facility

                # files that contain '1-min-avg.nc' get their own sub folder
                if "1-min-avg" in fname:
                    targetDir = targetDir+ "/1-min-avg"

                error = None

                if(not os.path.exists(targetDir)):                    
                    
                    try:
                        os.makedirs(targetDir)
                        #od = os.popen("chmod -R g+w " + targetDirBase)
                        #os.popen("chgrp -R " +self.datasetGroup+" " + targetDirBase)
                    except:
                        print "Failed to create directory " + targetDir 
                        self.errorFiles.append("Failed to create directory " + targetDir )
                        error = 1

                if not error:
                    targetFile = targetDir+'/'+fname

                    # see if file exists
                    if(not os.path.exists(targetFile)):
                        #try:
                            shutil.copy(theFile,targetFile )
                            print theFile +" created in -> "+ targetDir
                            os.popen("chmod g+w " + targetFile).readline()
                            #os.popen("chgrp "+self.datasetGroup +" " + targetFile).readline()
                            self.newCount += 1;
                        #except:
                        #    print "Failed to create file (" + theFile + "). check permissions and file name"
                        #    self.errorFiles.append("Failed to create file (" + theFile + "). check permissions and file name")

                    
                    # copy if more recent or rubbish file
                    elif (modified > os.stat(targetFile)[stat.ST_MTIME] + self.timezone_offset) or (os.path.getsize(targetFile) == 0):
                        try:
                            if os.path.getsize(targetFile) == 0:
                                print   "Zero sized file found: " + targetFile
                            #shutil.copy dosent seem to overwrite so delete then write
                            try:
                                    os.remove(targetFile)
                            except os.error:
                                    print "remove wasnt successfull"
                            try:
                                    shutil.copy(theFile,targetFile )
                                    print theFile +" updated in -> "+ targetDir
                                    self.updatedCount += 1;
                            except:
                                    print "copy of " + theFile + " wasnt successfull"
                                    
                                    

                        except Exception, e:
                            msg = "Failed to update file (" + theFile + " "  +  time.ctime() + ")  " + str(e)
                            self.errorFiles.append(msg)
                    else:
                        #print theFile +" checked ok -> "+ targetDir
                        self.checkedCount += 1;

                    #os.popen("chmod g+w " + targetFile).readline()
                    #os.popen("chgrp "+self.datasetGroup +" " + targetFile).readline()

            else:
                if code != "SOFS": # SOFS = bogus files writen by CSIRO. ignore them
                    err = "Unrecognised file "+ root+"/"+ fname + " with code '"  + code + "' found by the filesorter"
                    self.errorFiles.append(err)
                    # common error that needs  our attention
                    email = sendEmail.sendEmail()
                    email.sendEmail("pmbohm@utas.edu.au","SOOP File sorter- Unrecognised ship code",err)
        else:
            #print "Unrecognised file "+ root+"/"+ fname + " found by the filesorter"
            err = "Ignoring file "+ root+"/"+ fname + " not in agreed format"
            self.errorFiles.append(err)

    def writetoLog(self, report):
        filename = "SOOPFileSort_Report"
        os.chdir(self.localBaseDir)

        if    os.path.isfile(filename+ ".txt"):
            size =    os.path.getsize(filename+ ".txt")
            if size > 1000000:
                os.rename(filename + ".txt", filename +"_"+ time.strftime('%x').replace('/','-') + ".txt")
                None
            #print size
        log_file    =    open(filename+ ".txt",'a+')
        log_file.write("\r\nDownload time: " +  time.ctime() + "\r\n")
        for line in report:
            log_file.write(line + "\r\n")
        log_file.close()

    def close(self):
            try:
                self.ftp.quit()
            except:
                pass


if __name__ == "__main__":
    
        df = StorageConnection.StorageConnection() 
        df.connectStorage()
        
        filesort = soop_FileSort()
        #            processFiles(self,useDataFabric,origDir,userDestDir,fileExtension):
        filesort.processFiles(False, "/mnt/xvdb1/SOOP_cache/ships/","","nc")
      
        filesort.close()
        

