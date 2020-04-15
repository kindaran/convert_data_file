import logging
import sys
import os
import glob

import json
import pandas as ps

##########
## Classes
##########
class DataFile:

    def __init__(self,key,fileContent):

        try:
            self.__loadConfig(key,fileContent)
        except Exception as e:
            msg = str(e)
            logging.error("*****Error in Datafile.init(). Error: %s" % (msg))
    #END DEF

    def __loadConfig(self,key,fileContent):
        try:
            self.fileType = key
            self.filePrefix = fileContent["file_prefix"]
            self.sourcePath = fileContent["source_dir"]
            self.destPath = fileContent["target_dir"]
            self.archivePath = fileContent["archive_dir"]
            self.fileFormat = fileContent["file_format"]
            self.delimiter = fileContent["delimeter"]
        except Exception as e:
            msg = str(e)
            logging.error("*****Error in Datafile.__loadConfig(). Error: %s" % (msg))
    #END DEF
            
    def __getFileList(self,p_path):
        try:
            logging.debug("***GETTING INPUT DIR FILE LIST")
            filelist = [file for file in glob.glob(p_path + r"\*") if
                        not os.path.isdir(file) and not "_OUTPUT" in file.upper()]
            logging.debug("Found %s files in directory %s" % (len(filelist), p_path))
            if len(filelist) > 0:
                return filelist
            else:
                logging.warning("***WARNING: no files found in path %s matching search criteria" % (p_path))
                return None
            # end if
        except Exception as e:
            msg = str(e)
            logging.error("*****Error in Datafile.__getFileList. p_path: %s  Error: %s" % (p_path, msg))
            return None

    def searchSource(self):
        try:
            logging.info("*****LOOKING FOR FILES")
            #get file list using FileTypes filters
            fileList = self.__getFileList(self.sourcePath) 
            if fileList == None:
                logging.info("Unable to find files in source - exiting")
                return False
            #END IF
        
            self.fileList = [file for file in fileList if self.filePrefix.upper() + "_" in file.upper()]
            logging.debug("Filelist: %s" % (self.fileList))
            return True
        except Exception as e:
            msg = str(e)
            logging.error("*****Error in Datafile.searchSource(). Error: %s" % (msg))
            return False
    #END IF

    def loadData(self):

        self.fileData = []

        try:
            logging.info("*****LOADING FILE DATA")

            for file in self.fileList:
                if self.fileFormat == "delimited":
                    self.fileData.append(ps.read_csv(file,delimiter=self.delimiter,encoding='latin'))
                elif self.fileFormat == "excel":
                    self.fileData.append(ps.read_excel(file))
                else:
                    logging.error("Unknown file format %s - exiting" %(self.fileFormat))
                    return False
                #END IF
            #END FOR
            logging.debug("File data:\r\n %s" %(self.fileData))
            return True
        except Exception as e:
            msg = str(e)
            logging.error("*****Error in Datafile.searchSource(). Error: %s" % (msg))
            return False
    #END DEF

#END CLASS

##########
## Functions
##########
def getArgs():

    args = []

    try:
        logging.debug("***COMMAND LINE ARGS HARD CODED")
        if len(sys.argv) == 1:
            configFilePath = "." + "\\" # sys.argv[1] + "\\"
            args.append(configFilePath)
            configFileName = "banks.json" #sys.argv[2]
            args.append(configFileName)
            logging.debug("Config file path would be %s" %(configFilePath + configFileName))
            return args
        else:
            logging.error("Not enough arguments provided.")
            print("Incorrect arguments provided\r\nPlease include path to config file and file name")
            return None
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in getArgs. Args: %s  Error: %s" % (sys.argv,msg))
        return None
#END DEF
    
def loadConfigFile(p_path):

    data = None

    try:
        with open(p_path,'r') as read_file:
            data = json.load(read_file)
        #END WITH

        logging.debug(json.dumps(data))
        return data
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in loadConfigFile. Path: %s  Error: %s" % (p_path,msg))
        return None

#END DEF
  
##########
## Main
##########
def main():

    dataFile = []
    args = []

    try:
        ##get command line args
        logging.info("*****GET COMMAND LINE ARGS")
        args = getArgs()
        if args == None:
            logging.info("Unable to retrieve command line arguments - ending")
            return
        #END IF
        configFilePath = args[0] + args[1]

        #process JSON config file
        logging.info("*****LOAD CONFIG FILE")
        configFile = None
        configFile = loadConfigFile(configFilePath)
        if configFile == None:
            logging.info("Unable to retrieve config file - ending")
            return
        #END IF

        logging.info("*****PROCESS CONFIG FILE")
        #instantiate DataFile objects - one for each "file type" in the config file
        dataFile = [DataFile(fileType,configFile[fileType]) for fileType in configFile]
        logging.debug("Data file types: %s" %([file.fileType for file in dataFile]))

        #main loop
        logging.info("*****MAIN PROCESSING LOOP")
        for file in dataFile:
            logging.info("Processing file type %s" %(file.fileType))
            if not file.searchSource():
                logging.warning("Unable to find any source file for current file type")
                continue
            #END
            if not file.loadData():
                logging.warning("Unable to load data for current file type")
                continue
            #END IF
            

        #END FOR

    except Exception as e:
        msg = str(e)
        logging.error("*****Error in Main. Error: %s" % (msg))      
#END DEF


##########
## Globals
##########


##########
## Code Start
##########
loggingLevel = logging.DEBUG

logging.basicConfig(level=loggingLevel, format="%(levelname)s: %(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logging.info('*****PROGRAM START*****') 

if __name__ == "__main__":
    main()
#END IF

logging.info('*****PROGRAM END*****')  



