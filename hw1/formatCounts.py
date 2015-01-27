import csv

with open('temp_token_count.csv', 'r') as inputFile, open('token_counts.csv', 'w') as outputFile, open('popular_names.txt', 'r') as nameFile, open('name_counts.csv', 'w') as nameCount:
    csvtokenWriter = csv.writer(outputFile)
    csvtokenWriter.writerow(['token', 'count'])

    #build hashtable
    name = nameFile.readline()
    nameList = dict()
    while name:
        nameList[name.strip().lower()] = 0
        name = nameFile.readline()

    line = inputFile.readline()
    while line:
        count, token = line.strip().split()
        csvtokenWriter.writerow([token, count])
        if (token.lower() in nameList):
            nameList[token.lower()] = int(count)

        line = inputFile.readline()

    nameWriter = csv.writer(nameCount)
    nameWriter.writerow(['token', 'count'])
    sortedNameCount = sorted(nameList, key=nameList.__getitem__)
    for item in reversed(sortedNameCount):
        count_write = nameList[item]
        name_write = item.capitalize()
        if (count_write != 0):
            nameWriter.writerow([name_write, count_write])
        

    
