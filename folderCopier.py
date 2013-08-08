#!/bin/env python
# -*- coding: utf-8 -*-
import os, shutil, stat, time, grp, struct, binascii,sys
from os.path import  getmtime
from subprocess import Popen, PIPE, STDOUT



class folderCopier:

    def __init__(self):

        """
        Recursive folder copier
        Why isnt this script using rsync or scp?
        This script is designed for Datafabric idosyncrasies (datafabric connection handled elsewhere)
        Brute force delete and then copy when the target file is older

        """


        # place for log files.
        self.localBaseDir = os.getcwd()

        # default diresctory to move files to if targetDir does not start with slash /
        # full path from root. no trailing slash
        self.defaultDir = "/tmp"

        #self.datasetGroup = "pmbohm" # the linux group must exist
        self.script_time =  time.strftime("%a, %d %b %Y %H:%M:%S",time.localtime())

        self.fileTypeDefault = "" # no 'dot' this catch wont work on files without a file extension
        self.fileNameText2Find =  "" # only filenames containing this string will be copied (if nonempty string)

        self.datasetGroup = "pmbohm"

        # Use this to convert target modified time into local time
        self.timezone_offset = time.altzone

        self.newCount = 0
        self.checkedCount = 0
        self.checkedOKCount = 0
        self.updatedCount = 0
        self.fileOutput = []
        self.status = []
        self.errorFiles = []

    def processFiles(self,origDir,targetDir, fileType):

        if fileType == "":
            fileType = self.fileTypeDefault

        self.fileTypeDefault = fileType

        os.chdir(origDir)
        for root, dirs, fname in os.walk(os.getcwd()):
            fname.sort()
            for fname in fname:
                # looking for particular files
                self.checkedCount += 1
                if len(fileType) > 0:
                        if (fname.find(".") != -1):
                                if fname.rsplit(".",1)[1] != fileType:
                                        continue
                        else:
                                continue
                        # screen out files not containing text
                        if (self.fileNameText2Find != ""):
                                 if fname.find(self.fileNameText2Find) == -1:
                                        continue                               
                

                modified = os.stat(root+"/"+fname)[stat.ST_MTIME]
                self.handleFiles(fname,modified,root,origDir,targetDir)


        self.writetoLog()


    def handleFiles(self,fname,modified,root,origDir,targetDir):

        prevDirectory = os.getcwd()
        error = None
        theFile = root+"/"+fname

        if targetDir == "":
            targetDir = self.defaultDir
        elif  not targetDir.startswith("/"):
            targetDir = self.defaultDir + "/" + targetDir


        # strip trailing slash '/'
        if len(targetDir) > 1:
            if (targetDir.endswith("/")):
                targetDir = targetDir[:-1]

        if (targetDir.startswith("/")):
            os.chdir("/")

        targetDir += root.replace(origDir,"")

        print "Processing filename " + fname + " in dir: " + root
        #print "will write to " + targetDir


        if(not os.path.exists(targetDir)):

            os.makedirs(targetDir)
            od = os.popen("chmod -R g+w " + targetDir)
            os.popen("chgrp -R " +self.datasetGroup+" " + targetDir)
            try:
                pass
            except:
                print "Failed to create directory " + targetDir
                self.errorFiles.append("Failed to create directory " + targetDir )
                error = 1

        if not error:
            targetFile = targetDir+'/'+fname


            if(os.path.exists(targetFile)):
                fileModTime = time.gmtime(getmtime(targetFile))


                # TEST AGAINST SERVERS WITH UTC TIME BEFORE REMOVING THE NEXT SECTION
                if time.mktime(fileModTime) < time.mktime(time.gmtime(modified)):
                #print "then copy file as: " + str(time.mktime(fileModTime))+" is less than " +str(time.mktime(time.gmtime(modified)))
                    pass
                else:
                    #print "Ignoring update as: " + str(time.mktime(fileModTime))+" is not less than " +str(time.mktime(time.gmtime(modified)))
                    pass


            # see if file exists
            if(not os.path.exists(targetFile)):
                try:
                    shutil.copy(theFile,targetFile )
                    print theFile +" created in -> "+ targetDir
                    os.popen("chmod g+w " + targetFile.replace(' ','\ ')).readline()
                    os.popen("chgrp "+self.datasetGroup +" " + targetFile.replace(' ','\ ')).readline()
                    self.newCount += 1;
                except:
                    print "Failed to create file (" + theFile + "). check permissions and file name"
                    self.errorFiles.append("Failed to create file (" + theFile + "). check permissions and file name")

            # copy if more recent
            elif time.mktime(time.gmtime(getmtime(targetFile))) < time.mktime(time.gmtime(modified)):

                try:
                    #shutil.copy dosent seem to overwrite so delete then write
                    os.remove(targetFile)
                    shutil.copy(theFile,targetFile )
                    print theFile +" updated in -> "+ targetDir
                    self.updatedCount += 1;
                except Exception, e:
                    msg = "Failed to update file (" + theFile + " "  +  time.ctime() + ")  " + str(e)
                    self.errorFiles.append(msg)
            else:
                #print theFile +" checked ok -> "+ targetDir
                self.checkedOKCount += 1;


        # change back to processFiles directory
        os.chdir(prevDirectory)








    def writetoLog(self):

        self.status.append("\nSummary for folderCopier.py - " + self.script_time)
        self.status.append("Copying '"+ self.fileTypeDefault + "' files")
        self.status.append(str(self.checkedCount) + " Processed all files: ")
        self.status.append(str(self.newCount) + " New files: ")
        self.status.append(str(self.checkedOKCount) + " Existing files OK: ")
        self.status.append(str(self.updatedCount) + " Updated files: ")
        self.status.append(str(len(self.errorFiles)) + " Problems: ")
        for f in self.errorFiles:
            self.status.append(f)
            self.status.append("==============================")

        # print to console
        for f in self.status:
            print f
        filename = "folderCopier_Report"
        os.chdir(self.localBaseDir)

        if  os.path.isfile(filename+ ".txt"):
            size =  os.path.getsize(filename+ ".txt")
            if size > 1000000:
                os.rename(filename + ".txt", filename +"_"+ time.strftime('%x').replace('/','-') + ".txt")
                None
        print "Writing Log file: " +  self.localBaseDir + "/" +  filename + ".txt"
        log_file    =    open(self.localBaseDir + "/" +filename+ ".txt",'a+')
        for line in self.status:
            log_file.write(line + "\r\n")
        log_file.close()

    def close(self):
        try:
            self.ftp.quit()
        except:
            pass


if __name__ == "__main__":
    """
    usage:
    f.processFiles("/home/pmbohm/MATLAB","anything",'')
    PARAM 1: [/home/pmbohm/MATLAB] this folders contents will be copied
    PARAM 2: [anything] name of the relative folder (to self.defaultDir) the contents of MATLAB
             [/anything] would copy to absolute path
    PARAM 3: file extension. blank for all files
    """
    f = folderCopier()

    #f.processFiles("/home/pmbohm/grag/grag-1.1","/tmp",'txt')
    f.close()
