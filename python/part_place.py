'''
Author: 刘昕 2914907975@qq.com
Date: 2023-06-30 14:53:11
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-06-30 15:11:07
FilePath: \python\part_place.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
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