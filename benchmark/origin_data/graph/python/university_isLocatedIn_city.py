
import sys

filePath = sys.argv[1]
file = open(filePath + "university.tbl","r", encoding='utf8')
file1 = open(filePath + "organisation_isLocatedIn_place.tbl","r", encoding='utf8')
edge_file = open(filePath + "university_isLocatedIn_city.tbl", "w", encoding='utf8')
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
    if list[0] in set:
        edge_file.write(fileStr)
file1.close()
edge_file.close()