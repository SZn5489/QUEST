'''
Author: 刘昕 2914907975@qq.com
Date: 2023-06-30 15:16:38
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-06-30 16:07:20
FilePath: \python\person_studyAt_university.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
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