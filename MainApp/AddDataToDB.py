import pyodbc
import json
import pandas as pd

import csv

##############################################################################
def ReadData():
    conn = pyodbc.connect("Driver={SQL Server};"
                          "Server=localhost\SQLEXPRESS;"
                          "Database=FederalReporterDB;""Trusted_Connection=yes;")
    sql = "select ProjectNumber, Department, Agency, IC_Center," \
          "ContactPIProjectLeader, OrganizationName, OrganizationCity," \
          " OrganizationState from sampletable"

    cursor = conn.cursor()
    cursor.execute(sql)
    dataList = []
    for row in cursor.fetchall():
        eachRow = {}
        eachRow['ProjectNumber'] = row.ProjectNumber
        eachRow['Department'] = row.Department
        eachRow['Agency'] = row.Agency
        eachRow['IC_Center'] = row.IC_Center
        eachRow['ContactPIProjectLeader'] = row.ContactPIProjectLeader
        eachRow['OrganizationName'] = row.OrganizationName
        eachRow['OrganizationCity'] = row.OrganizationCity
        eachRow['OrganizationState'] = row.OrganizationState
        dataList.append(eachRow)
    cursor.close()
    del cursor
    return json.dumps(dataList)

##############################################################################
def InsertData():
    try:
        dataList = ReadDataFromFile()
        nnx = pyodbc.connect("Driver={SQL Server};"
                              "Server=localhost\SQLEXPRESS;"
                               "Database=FederalReporterDB;""Trusted_Connection=yes;")
        df = pd.read_sql_query("select * from sampletable", nnx)
        cursor = nnx.cursor()

        for data in dataList:
            sql = ("insert into sampletable (ProjectNumber, Department, Agency, IC_Center, " \
                  " ContactPIProjectLeader, OrganizationName, OrganizationCity, OrganizationState) values(?,?,?,?,?,?,?,?);")
            Values =[data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]
            cursor.execute(sql, Values)
            nnx.commit()
            print("insert done")
    except:
        print("error")
    finally:
        cursor.close()

def ReadDataFromFile():
    fields = []
    rows=[]
    dataList =[]
    filename = 'C:/Users/cmoon/Downloads/FedRePORTER_PRJ_C_FY2019/FedRePORTER_PRJ_C_FY2019.csv'
    try:
        with open(filename, 'r') as csvFile:
            csvReader = csv.reader(csvFile, delimiter=',')
            fields = next(csvReader)
            for row in csvReader:
                savingRows=[]
                savingRows.append(row[6])
                savingRows.append(row[3])
                savingRows.append(row[4])
                savingRows.append(row[5])
                savingRows.append(row[9])
                savingRows.append(row[13])
                savingRows.append(row[14])
                savingRows.append(row[15])
                dataList.append(savingRows)
                if len(dataList) > 23:
                    break
        return dataList
    except:
        print('can not read file')
if __name__== "__main__":
    InsertData()