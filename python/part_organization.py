'''
Author: 刘昕 2914907975@qq.com
Date: 2023-06-30 14:53:22
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-06-30 15:15:47
FilePath: \python\part_organization.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
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