import hashlib
import sys
import os
import shutil
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path

#Assignment of Command line arguments to variables
if len(sys.argv) > 1:
    sourceDir = sys.argv[1]
    replicaDir = sys.argv[2]
    logFile = sys.argv[3]
    syncTime = int(sys.argv[4])
else:
    print("Please provide Source folder path, Replica folder path, log file path, time in seconds")
    exit(0)

#log file configuration
logging.basicConfig(filename=logFile, format='%(asctime)s %(message)s', filemode='a')
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
#Read logFile
with open(logFile, 'r') as file:
    log_lines = file.readlines()

def on_created(event):
    print("File: Source Event: Created " + event.src_path)
    logger.info("File: Source Event: Created " + event.src_path)
def on_deleted(event):
    print("File: Source Event: Deleted " + event.src_path)
    logger.info("File: Source Event: Deleted " + event.src_path)

def on_modified(event):
    print("File: Source Event: Modified " + event.src_path)
    logger.info("File: Source Event: Modified " + event.src_path)
def on_moved(event):
    print("File: Source Event: Renamed " + event.dest_path)
    logger.info("File: Source Event: Renamed " + event.dest_path)

# calculating md_5 of the file present in source and replica folder
def calc_md5(file_path, chunk_size=8192):
    hashMd5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hashMd5.update(chunk)
        return hashMd5.hexdigest()
    except Exception as e:
        print("Error calculating MD5 for '{file_path}': {e}")
    return None


class source():
    def run(self):
        detectEvent = FileSystemEventHandler()
        replicaObject = replica()
        detectEvent.on_created = on_created
        detectEvent.on_deleted = on_deleted
        detectEvent.on_modified = on_modified
        detectEvent.on_moved = on_moved
        observer = Observer()
        observer.schedule(detectEvent, sourceDir, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(syncTime)
                replicaObject.run()
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

class replica():
    def run(self):
        #Copies files which are not present in replica folder
        for sourceFile in os.listdir(sourceDir):
            sourceFilePath = os.path.join(sourceDir, sourceFile)
            replicaFilePath = os.path.join(replicaDir, sourceFile)
            sourceDirSize = 0
            replicaDirSize = 0
            sourceDirPath = Path(sourceFilePath)
            replicaDirPath = Path(replicaFilePath)
            if os.path.exists(replicaFilePath):
                if os.path.isdir(replicaFilePath):
                    sourceDirCount = sum(1 for _ in sourceDirPath.rglob('*'))
                    replicaDirCount = sum(1 for _ in replicaDirPath.rglob('*'))
                #Calculated folder size
                    for ele in os.scandir(sourceFilePath):
                        sourceDirSize += os.path.getsize(ele)
                    for ele in os.scandir(replicaFilePath):
                        replicaDirSize += os.path.getsize(ele)

                    #If size or count didn't match then source root folder gets copied in replica folder
                    if sourceDirSize != replicaDirSize or sourceDirCount != replicaDirCount:
                        try:
                            shutil.rmtree(replicaFilePath)
                            shutil.copytree(sourceFilePath, replicaFilePath)
                            print("File: Replica Event: Modified " + replicaFilePath)
                            logger.info("File: Replica Event: Modified " + replicaFilePath)
                        except Exception as e:
                            print("Error copying:", e)

                elif os.path.isfile(replicaFilePath):
                    sourceFilePathMd5 = calc_md5(sourceFilePath)
                    replicaFilePathMd5 = calc_md5(replicaFilePath)
                    if sourceFilePathMd5 != replicaFilePathMd5:
                        try:
                            os.remove(replicaFilePath)
                            shutil.copy(sourceFilePath, replicaFilePath)
                            print("File: Replica Event: Modified " + replicaFilePath)
                            logger.info("File: Replica Event: Modified " + replicaFilePath)
                        except Exception as e:
                            print("Error: " + e)
                            logging.exception(e)

            else:
                if os.path.isdir(sourceFilePath):
                    try:
                        shutil.copytree(sourceFilePath, replicaFilePath)
                        print("File: Replica Event: Created " + replicaFilePath)
                        logger.info("File: Replica Event: Created " + replicaFilePath)
                    except Exception as e:
                        print("Error copying:", e)
                elif os.path.isfile(sourceFilePath):
                    try:
                        shutil.copy(sourceFilePath, replicaFilePath)
                        print("File: Replica Event: Created " + replicaFilePath)
                        logger.info("File: Replica Event: Created " + replicaFilePath)
                    except Exception as e:
                        print("Error: " + e)
                        logging.exception(e)

        #Checking replica folder and deleting extra file which are not present in source folder
        for replicaFile in os.listdir(replicaDir):
            sourceFilePath = os.path.join(sourceDir, replicaFile)
            replicaFilePath = os.path.join(replicaDir, replicaFile)
            if os.path.isdir(replicaFilePath) and not os.path.exists(sourceFilePath):
                shutil.rmtree(replicaFilePath)
                print("File: Replica Event: Deleted " + replicaFilePath)
                logger.info("File: Replica Event: Deleted " + replicaFilePath)
            elif os.path.isfile(replicaFilePath) and not os.path.exists(sourceFilePath):
                os.remove(replicaFilePath)
                print("File: Replica Event: Deleted " + replicaFilePath)
                logger.info("File: Replica Event: Deleted " + replicaFilePath)


sourceObject = source()
sourceObject.run()
