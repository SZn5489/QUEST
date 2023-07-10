'''
Author: 刘昕 2914907975@qq.com
Date: 2023-06-30 15:16:24
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-06-30 15:59:45
FilePath: \python\city_isPartOf_country.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import sys

filePath = sys.argv[1]
file = open(filePath + "city.tbl","r", encoding='utf8')
file1 = open(filePath + "place_isPartOf_place.tbl","r", encoding='utf8')
edge_file = open(filePath + "city_isPartOf_country.tbl", "w", encoding='utf8')
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