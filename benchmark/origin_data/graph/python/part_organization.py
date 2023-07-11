import sys

filePath = sys.argv[1] 
file = open(filePath + "organisation.tbl","r", encoding='utf8')
university_file = open(filePath+ "university.tbl", "w", encoding='utf8')
company_file = open(filePath+ "company.tbl", "w", encoding='utf8')
file.readline()
while True:
    lineStr = file.readline()
    print(lineStr)
    if not lineStr:
        break
    list = lineStr.split("|")
    if list[1] == "university":
        university_file.write(lineStr)
    elif list[1] == "company":
        company_file.write(lineStr)
file.close()
university_file.close()
company_file.close()