import sys
import os
if __name__ == "__main__":
    filePath = sys.argv[1]
    destPath = sys.argv[2]
    for root, dirs, files in os.walk(filePath, topdown=False):
        for file in files:   
            if file[-3:] == "csv":
                print(file)
                fileName = file
                dstFileName = file[ :-8] + ".tbl"
                file = open(filePath+fileName,"r", encoding='utf8')
                dstFile = open(destPath+dstFileName, "w", encoding='utf8')
                lineStr = file.readline()
                while True:
                    lineStr = file.readline()
                    if not lineStr:
                        break
                    dstFile.write(lineStr)
                file.close()
                dstFile.close()