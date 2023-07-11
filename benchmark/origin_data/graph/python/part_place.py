
import sys

filePath = sys.argv[1] 
file = open(filePath + "place.tbl","r", encoding='utf8')
country_file = open(filePath+ "country.tbl", "w", encoding='utf8')
city_file = open(filePath+ "city.tbl", "w", encoding='utf8')
while True:
    lineStr = file.readline()
    print(lineStr)
    if not lineStr:
        break
    list = lineStr.split("|")
    if list[3] == "city\n":
        city_file.write(lineStr)
    elif list[3] == "country\n":
        country_file.write(lineStr)
file.close()
country_file.close()
city_file.close()