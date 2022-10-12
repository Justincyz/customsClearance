import pandas as pd
import os
import sys
import re
import databaseConnection
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


headerName = ["No.", "ChineseName", "EnglishName", "Quantity", "UnitPrice", "TotalPrice", "HSCode", "Material",
              "UseFor"]
ignorableFile = ('.WeDrive')
debugPath = r'C:\Users\yuzhuangchen\Desktop\errorFile'
workPath = r"C:\Users\yuzhuangchen\Documents\WXWork\1688858301406791\WeDrive\飞客国际\清关转运"


def main():
    retrieveTopKInfo = 200
    databaseConnection.initializeCustomsClearanceTable()
    for dirpath, dirnames, filenames in os.walk(workPath):
        for filename in filenames:
            if isIgnorableFile(filename):
                continue
            processingFile(dirpath, filename)
            # retrieveTopKInfo = retrieveTopKInfo - 1
            # if retrieveTopKInfo < 0:
            #     databaseConnection.closeDbConnection()
            #     print("Finished!")
            #     sys.exit(0)

    databaseConnection.closeDbConnection()
    print("Finished!")
def isIgnorableFile(filename):
    if filename.startswith('~') or filename.startswith('.'):
        return True
    if filename.endswith('xls') or filename.endswith('xlsx'):
        return False
    if re.match(r'.*实际申报.*', filename) or re.match(r'.*客户确认.*', filename):
        return False
    return True


def processingFile(dirpath, filename):
    print(filename)
    nameOfSequence = ('序号\nNo.', '单号\nNo.')

    #找到<发票>这一表格
    currentXlsxData = pd.read_excel(os.path.join(dirpath, filename),sheet_name=None)
    commercialInvoiceSheet = 0

    # 发票表单有可能用英文也可能用中文表达，同时也不能确定在第几个sheet里面
    for i in range(len(list(currentXlsxData))):
        if re.match(r'.*(发票|Commercial Invoice).*', list(currentXlsxData)[i]):
            commercialInvoiceSheet = i
            break

    currentXlsxData = pd.read_excel(os.path.join(dirpath, filename),
                                    sheet_name=commercialInvoiceSheet,
                                    usecols="A:K")

    # get the correct header information offset
    headerInfoOffset = 0
    headerChineseName = None
    headerEnglishName = None
    headerHSCode = None
    headerQuantity = None
    headerUnitPrice = None
    headerTotalPrice = None
    headerMaterial = None
    headerUseFor = None
    headerTaxRate = None
    for row in currentXlsxData.index:
        key = currentXlsxData.loc[row]
        # found the line with the header information

        if type(key[0]) is str and re.match(r'.*(序号|单号).*', key[0]):
            headerInfoOffset = row
            for header in currentXlsxData.values[row]:
                if pd.isnull(header):
                    continue
                elif re.match(r'.*中文品名.*', header):
                    headerChineseName = header
                elif re.match(r'.*英文品名.*', header):
                    headerEnglishName = header
                elif re.match(r'.*海关编码.*', header):
                    headerHSCode = header
                elif re.match(r'.*数量.*', header):
                    headerQuantity = header
                elif re.match(r'.*单价.*', header):
                    headerUnitPrice = header
                elif re.match(r'.*总价.*', header):
                    headerTotalPrice = header
                elif re.match(r'.*材质.*', header):
                    headerMaterial = header
                elif re.match(r'.*(用途).*', header):
                    headerUseFor = header
                elif re.match(r'.*(税率|Duty).*', header):
                    headerTaxRate = header
            break


    #the next row would be our targeting body information
    headerInfoOffset = headerInfoOffset + 1
    currentXlsxData = pd.read_excel(os.path.join(dirpath, filename),
                                    sheet_name=0,
                                    header=headerInfoOffset,
                                    usecols="A:K")


    AfterModified = "1" if filename.startswith('实际申报') else "0"

    for row in currentXlsxData.index:
        key = currentXlsxData.loc[row]
        #check if body data reach the EOF
        if type(key[0]) is not int:
            break
        HSCode = "NULL"
        Material = "NULL"
        UseFor = "NULL"
        TaxRate = "NULL"
        # only retrieve useful data
        key = currentXlsxData.loc[row]
        MBLNumber = getMBLNumber(dirpath, filename)
        # means this file is not complete yet, since 10
        if headerChineseName == None:
            break
        ChineseName = str(key[headerChineseName])

        if headerEnglishName == None:
            break
        EnglishName = str(key[headerEnglishName])
        Quantity = str(key[headerQuantity])
        UnitPrice = str(key[headerUnitPrice])
        TotalPrice = str(key[headerTotalPrice])
        if headerHSCode is not None:
            HSCode = str(key[headerHSCode])
            HSCode = HSCode[0:-2] # remove extra .0 from the tail
        if headerTaxRate is not None:
            TaxRate = str(key[headerTaxRate])
        if headerUseFor is not None:
            UseFor = str(key[headerUseFor])
        if headerMaterial is not None:
            Material = str(key[headerMaterial])

        entryInfo = "INSERT INTO development.CustomsClearance(MBLNumber, FileName, ChineseName, EnglishName, AfterModified, HSCode, " \
                    "Quantity, UnitPrice, TotalPrice, Material, UseFor, TaxRate) \
                 VALUES (%s, %s,%s, %s,  %s,  %s,  %s, %s, %s,  %s,  %s,  %s)" %\
                    ("\""+MBLNumber+"\"", "\""+str(filename)+"\"", "\""+ChineseName+"\"", "\""+EnglishName+"\"", "\""+AfterModified+"\"", "\""+HSCode+"\"",
                     "\""+Quantity+"\"", "\""+UnitPrice+"\"", "\""+TotalPrice+"\"", "\""+Material+"\"",
                     "\""+UseFor+"\"", TaxRate)

        databaseConnection.saveProductInformation(entryInfo)
        headerInfoOffset = headerInfoOffset + 1


def getMBLNumber(dirpath, filename):
    MBLNumber = "NULL"
    currentXlsxData = pd.read_excel(os.path.join(dirpath, filename),
                                    sheet_name=0,
                                    usecols="A:K")
    for row in currentXlsxData.index:
        key = currentXlsxData.loc[row]
        if type(key[0]) != str:
            continue
        if re.match(r'.*(B/L|MBL|BL).*', key[0]):
            for r in range(1, len(currentXlsxData.values[row])):
                if not pd.isnull(currentXlsxData.values[row][r]):
                    draftMBLNumber = str(currentXlsxData.values[row][r])
                    draftMBLNumberSplit = re.split('：|:', draftMBLNumber)

                    if len(draftMBLNumberSplit) > 1: #means extra sentence that seperated by ':'
                        MBLNumber = draftMBLNumberSplit[1]
                    else:
                        MBLNumber = draftMBLNumber
                    break
            if MBLNumber is not None:
                break

    return str(MBLNumber)

main()