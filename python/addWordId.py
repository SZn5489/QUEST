'''
Author: 刘昕 2914907975@qq.com
Date: 2023-07-07 20:32:44
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-07-07 20:36:02
FilePath: \python\addWordId.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''

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
