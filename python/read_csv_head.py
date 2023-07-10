'''
Author: SZn5489 2914907975@qq.com
Date: 2023-06-19 20:12:27
LastEditors: 刘昕 2914907975@qq.com
LastEditTime: 2023-06-29 16:10:38
FilePath: \python\read_csv_head.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import sys
if __name__ == "__main__":
    filePath = sys.argv[1]
    file = open(filePath,"r", encoding='utf8')
    lineStr = file.readline()
    print(lineStr)
    i = 0
    while(i < 10):
        i += 1
        lineStr = file.readline()
        print(lineStr)
    
    
    file.close()