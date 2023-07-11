
import sys

filePath = sys.argv[1]
targetPath = sys.argv[2] 
file = open(filePath,"r", encoding='utf8')
newFile = open(targetPath, "w", encoding='utf8')

line = file.readline()
while line:
    lineList = line.split("|")
    newLine = lineList[0] + "|" + lineList[1] + "\n"
    newFile.write(newLine)
    line = file.readline()
    
newFile.close()
file.close()