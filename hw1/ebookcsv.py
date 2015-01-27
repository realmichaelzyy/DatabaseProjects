#this is a helper python file created by Daxi Li

"""
import fileinput

for line in fileinput.input()
"""
import sys
import csv

#will add parameter checking when have time
def extract_filename ():
    filename = sys.argv[1]
    return filename

fileName = extract_filename()

with open('test.csv', 'w') as outputFile and open(fileName, 'r') as inputFile:
    csvWriter = csv.writer(outputFile)
    csvWriter.writerow(['title','author','release_date','ebook_id','language', 'body'])
    
    title = author = release_date = ebook_id = language = "null"
    body = "There must be a body field! What happen?"
    
    line = inputFile.readline()
    while line:
        if 'title:' in line.lower():
            index = line.find(':')
            content = line[index+1:].strip()
            if content != "":
                title = content
        elif 'author:' in line.lower():
            index = line.find(':')
            content = line[index+1:].strip()
            if content != "":
                author = content
        elif 'release date:' in line.lower():
            index = line.find(':')
            content = line[index+1:].strip()
            if content != "" and '[' in line and '#' in line and ']' in line:
                index2 = line.find('[')
                index3 = line.find('#')
                index4 = line.find(']')
                release_date = line[index+1, index2].strip()
                ebook_id = line[lindex3+1: index4].strip()
        elif 'language:' in line.lower():
            index = line.find(':')
            content = line[index+1:].strip()
            if content != "":
                language = content
        elif '*** start of' in line.lower():
            line = inputFile.readline()
            firstTime = True
            while '*** end of' not in line.lower():
                if (firstTime):
                    body = line
                    firstTime = False
                else:
                    body += line
                line = inputFile.readline()
            csvWriter.writerow([title, author, release_date, ebook_id, language, body])
            title = author = release_date = ebook_id = language = "null"
            body = "There must be a body field! What happen?"

        line = inputFile.readline()


