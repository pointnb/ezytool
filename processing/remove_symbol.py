# -*- coding: utf-8 -*-

import sys
import re


def rm_symbol(line, symbol):
        for w in symbol:
                line = line.replace(w.strip(), "")
        return line

try:
	inputfile = sys.argv[1]
	outputfile = inputfile.replace(".txt", "_rm.txt")
	
	argc = len(sys.argv) - 1
	flag = 0

	if argc == 2 and sys.argv[2] == "-a":
		flag = 1
		symbol_file = open("symbol.txt", "r")
		symbol = []
		while 1:
			string = symbol_file.readline()
			if not string: break
			symbol.append(string)
		symbol_file.close()

except Exception:
	print 'remove_symbol.py [file name] [option]'
	print 'ex: python remove_symbol.py 2016_12_news.txt'
	print '[option] -a: remove all except Hangul'

else:
	if __name__ == '__main__':
		
		fin = open(inputfile, 'r')
		fout = open(outputfile, 'w')
		
		while 1:
			line = fin.readline()
			if not line: break
			
			if flag == 1:
				line = rm_symbol(line, symbol)
			else:
				line = re.sub("[^가-힣]", "", line) + "\n"
			fout.write(line)

		fin.close()
		fout.close()
