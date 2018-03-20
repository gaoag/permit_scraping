from multiprocessing import Pool
from time import sleep
import robobrowser
import re
import os
import csv
import shutil
import logging
import sys
import io
import csv_splitter

columnHeaders = ['Permit Number', 'Permit Type', 'Permit Type Definition', 'Permit Creation Date', 'Block', 'Lot', 'Street Number', 'Street Number Suffix', 'Street Name', 'Street Suffix', 'Unit', 'Unit Suffix', 'Description', 'Status', 'Status Date', 'Structural Notification', 'Number of Existing Stories', 'Number of Proposed Stories', 'Voluntary Soft-Story Retrofit', 'Fire Only Permit', 'Permit Expiration Date', 'Estimated Cost', 'Revised Cost', 'Existing Use', 'Existing Units', 'Proposed Use', 'Proposed Units', 'Plansets', 'TIDF Compliance', 'Use Codes', 'Use Code Description', 'Existing Construction Type', 'Existing Construction Type Description', 'Proposed Construction Type', 'Proposed Construction Type Description', 'Supervisor District', 'Neighborhoods - Analysis Boundaries', 'Zipcode', 'Location', 'Stages', 'Addenda', 'Appointments', 'Inspections', 'Special']

#file system cleanup/setup
shutil.rmtree('../datafiles/SFPermitData/Split/')
os.mkdir('../datafiles/SFPermitData/Split/')
shutil.rmtree('../datafiles/SFPermitData/SplitExtended/')
os.mkdir('../datafiles/SFPermitData/SplitExtended/')

def get_housing_data(fileName):
    tableIds = {"Addenda":"InfoReq1_dgAddendaDetails", "Stages":"InfoReq1_dgStages", "Appointments":"InfoReq1_dgAppointmentDetails",
    "Inspections":"InfoReq1_dgInspectionDetails", "Special":"InfoReq1_dgPtsSpInspDetails"}

    readFile = io.open('../datafiles/SFPermitData/Split/'+fileName, 'r', encoding='windows-1252')
    reader = csv.DictReader(readFile)
    writeFile = open('../datafiles/SFPermitData/SplitExtended/'+fileName[:-4]+'extended.csv', 'w')
    headerWriter = csv.writer(writeFile, lineterminator='\n')
    headerWriter.writerow(columnHeaders)
    writer = csv.DictWriter(writeFile, lineterminator='\n', fieldnames=columnHeaders)
    z = 0
    for fileRow in reader:

        #obtain permit number
        permitNumber = fileRow['Permit Number']
        url = 'http://dbiweb.sfgov.org/dbipts/default.aspx?page=Permit&PermitNumber=' + permitNumber

        br = robobrowser.browser.RoboBrowser()

        br.open(url)
        defaultForm = br.get_form(id="DefaultForm")
        br.submit_form(defaultForm, submit=defaultForm['InfoReq1$btnPrintSitePermit'])

        rowToWrite = dict(fileRow)

        #parse each table table into our data format
        for name, id in tableIds.items():
            table = br.find("table", attrs={"id":id})

            if table is None:
                logging.info('problem permit: ' + permitNumber)
                logging.info('problem table: ' + id)
                continue

            headings = [th.get_text() for th in table.find("tr").find_all("th")]
            tableData = []
            for row in table.find_all("tr")[1:]:
                rowValues = [re.sub(r'[\r\n\t\xa0]', '', td.get_text()) for td in row.find_all("td")]
                rowDict = {}
                for i in range(0, len(rowValues)):
                    rowDict[headings[i]] = rowValues[i]
                tableData.append(rowDict)
            rowToWrite[name] = str(tableData)

        writer.writerow(rowToWrite)

        #LIMITER FOR THE ENTRIES - DELETE FOLLOWING 3 LINES TO REMOVE LIMITER
        z += 1
        sleep(3)
        print(permitNumber)
    readFile.close()
    writeFile.close()

if __name__ == '__main__':

    #set up logging, reset our log file
    if os.path.isfile('../scripts/permit_scrape.log'):
        os.remove('../scripts/permit_scrape.log')
    logging.basicConfig(filename='../scripts/permit_scrape.log',level=logging.INFO)

    pool = Pool()
    #step 1 - setup (write the headers) and close the files
    readFile = open('../datafiles/SFPermitData/Building_Permits.csv', 'r')
    reader = csv.DictReader(readFile)
    newFilePath = '../datafiles/SFPermitData/Building_Permits_Extended.csv'
    if os.path.isfile(newFilePath):
        os.remove(newFilePath) #prevents us from re-editing same file
    writeFile = open(newFilePath, 'w')
    headerWriter = csv.writer(writeFile, lineterminator='\n')
    headerWriter.writerow(columnHeaders)

    readFile.close()
    writeFile.close()

    #after initial setup, split the files
    csv_splitter.split(open('../datafiles/SFPermitData/Building_Permits.csv', 'r'), output_path='../datafiles/SFPermitData/Split/')
    #get housing data for all the split files
    pool.map(get_housing_data, [f for f in os.listdir('../datafiles/SFPermitData/Split/')])

    #now, write them back to the original
    writeFile = open(newFilePath, 'w')
    writer = csv.DictWriter(writeFile, lineterminator='\n', fieldnames=columnHeaders)
    for fileName in os.listdir('../datafiles/SFPermitData/SplitExtended/'):
        readFile = open('../datafiles/SFPermitData/SplitExtended/' + fileName)
        reader = csv.DictReader(readFile)
        for row in reader:
            toWrite = {}
            for column in columnHeaders:
                toWrite[column] = row[column]
            writer.writerow(toWrite)
        readFile.close()
    writeFile.close()
