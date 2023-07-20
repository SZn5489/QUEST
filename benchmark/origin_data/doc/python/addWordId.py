import sys

filePath = sys.argv[1]
wordPath = sys.argv[2]
file = open(filePath,"r", encoding='utf8')
newWordFile = open(wordPath, "w", encoding='utf8')

line = file.readline()
index = 1
while line:
    newLine = str(index) + "|" + line
    index = index+1
    newWordFile.write(newLine)
    line = file.readline()
    
newWordFile.close()
file.close()
