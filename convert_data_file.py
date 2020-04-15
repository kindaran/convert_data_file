import logging
import sys

import json

##########
## Classes
##########
class DataFile:

    def __init__(self,key,fileContent):
        self.fileType = key
        self.filePrefix = fileContent["file_prefix"]

    #END DEF
#END CLASS

##########
## Functions
##########
def loadConfigFile(p_path):

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

    fileTypes = []

    try:
        ##get command line args
        logging.debug("***COMMAND LINE ARGS HARD CODED")
        if len(sys.argv) == 1:
            configFilePath = "." + "\\" # sys.argv[1] + "\\"
            configFileName = "banks.json" #sys.argv[2]
            logging.debug("Config file path would be %s" %(configFilePath + configFileName))
        else:
            logging.error("Not enough arguments provided.")
            print("Incorrect arguments provided\r\nPlease include path to config file and file name")
            return
        #END IF

        #process JSON config file
        configFile = None
        configFile = loadConfigFile(configFilePath + configFileName)
        if configFile == None:
            logging.info("Unable to retrieve config file - ending")
            return
        #END IF

        for fileType in configFile:
            fileTypes.append(DataFile(fileType,configFile[fileType]))
        #END FOR

        if logging.root.level == logging.DEBUG:
            logging.debug("Config file types")
            for fileType in fileTypes:
                logging.debug("%s" %(fileType.fileType))
                logging.debug("%s" %(fileType.filePrefix))
        #END IF

        filePrefixes = [fileType.filePrefix for fileType in fileTypes]
        logging.debug("Prefixes: %s" %(filePrefixes))
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
logging.info('*****PROGRAM START') 

if __name__ == "__main__":
    main()
#END IF

logging.info('*****PROGRAM END')  



