import sys

fin = open(sys.argv[1], 'r')
fout = open(sys.argv[2], 'w')
_type = sys.argv[3]

while 1:
	line = fin.readline()
	if not line: break
	if line[0] == '@': continue
	data = line.replace('\n', '').split("@")

	body = data[0]
	year = data[1]
	month = data[2]

	indexStr = "{\"index\": {\"_index\": \"baseball\", \"_type\": \"" + _type + "\"} }"
	bodyStr = "{\"body\": \"" + body + "\", \"year\": \"" + year + "\", \"month\": \"" + month + "\", \"date\": \"" + year + "-" + month + "\"}"
	
	fout.write(indexStr + "\n")
	fout.write(bodyStr + "\n")

fin.close()
fout.close()



