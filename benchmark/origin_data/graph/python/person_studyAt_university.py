import sys

filePath = sys.argv[1]
file = open(filePath + "university.tbl","r", encoding='utf8')
file1 = open(filePath + "person_studyAt_organisation.tbl","r", encoding='utf8')
edge_file = open(filePath + "person_studyAt_university.tbl", "w", encoding='utf8')
set = set()
while True:
    fileStr = file.readline()
    if not fileStr:
        break
    list = fileStr.split("|")
    set.add(list[0])
file.close()

while True:
    fileStr = file1.readline()
    if not fileStr:
        break
    list = fileStr.split("|")
    if list[1] in set:
        edge_file.write(fileStr)
file1.close()
edge_file.close()