import keyring, time, sys, string # PEP, meh.
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import MySQLConnection, Error
from getCredentials import gc

# Set manufacturer name
manufacturer_name = 'Titech'
manufacturer_id = 17038

print("editing: " + manufacturer_name)
print(manufacturer_name)
print(" ")
# Replace *** with osx keychain context
pvtpath = "~/Dropbox/iPython/Outputs/"
cred = gc()

dbconfig = {
    "database": cred.get("myd"),
    "user": cred.get("mydu"),
    "password": cred.get("mydp"),
    "use_unicode": "True",
    "buffered":"True",
}

# Get status quo
SQEquipmentQuery ="""
SELECT asset.name AS asset, asset.id AS assetID
FROM asset
WHERE asset.name LIKE '%{0}%'
""".format(manufacturer_name)

def getSQEquipment():
    db_connection_sales_mgr = mysql.connector.connect(**dbconfig)
    SQequipment = pd.read_sql(SQEquipmentQuery, db_connection_sales_mgr)
    return SQequipment

SQequip = getSQEquipment()

print("Rows: " + str(len(SQequip.index)))
oldrows = len(SQequip.index)
print(SQequip)

# Report action
print("Finding equipment matching: %s" % manufacturer_name)
print("Setting MFG to: %d" % manufacturer_id)

# Get user approval
approval = input("Proceed? (Yes/No) ")
if approval == 'Yes':
    pass
else:
    print("Approval declined and aborting script")
    sys.exit()



# Construct via CONCAT string for classification based on attributes that contribute quality listing.
equipmentQuery ="""
SELECT asset.name AS asset, asset.id AS assetID
FROM asset
WHERE asset.name LIKE '%{0}%'
""".format(manufacturer_name)

def getEquipment():
    db_connection_sales_mgr = mysql.connector.connect(**dbconfig)
    equipment = pd.read_sql(equipmentQuery, db_connection_sales_mgr)
    return equipment

####### run query and get assets where MFG name should be changed
equip = getEquipment()

####### save this asset list to a log file in-case we need to revert
equip.to_csv(pvtpath + 'MFG_name_change_log.csv')
####### convert equip DataFrame to a Python List
assetIDList = equip['assetID'].values.tolist()
####### convert Python list to a comma delimmited string
assetIDString = ','.join(str(e) for e in assetIDList)

###### Create query for updating the manufacturer
equipmentUpdateQuery ="""
UPDATE asset
SET asset.manufacturer_client_id = {1}
WHERE asset.id IN ({2})
""".format(0,manufacturer_id,assetIDString)

###### Define connection for sending the manufacturer
def updateMFGQuery():
    cnx = mysql.connector.connect(**dbconfig)
    cursor = cnx.cursor()
    cursor.execute(equipmentUpdateQuery)
    cnx.commit()
    cursor.close()
    cnx.close()

# Send the MFG update
updateqmfg = updateMFGQuery()

try:
    for index, row in equip.iterrows():
        k = 'asset'
        v = row[1]
        print("inserting: ", k, v)
        fillQueueQuery = """
        INSERT INTO website_api_sync_queue (entity_type, entity_id)
        VALUES ('asset', {3})
        """.format(0,0,0,v)
        print(fillQueueQuery)
        ###### Define connection for pushing to the website queue
        def updateWebsiteQuery():
            cnx = mysql.connector.connect(**dbconfig)
            cursor = cnx.cursor()
            cursor.execute(fillQueueQuery)
            cnx.commit()
            cursor.close()
            cnx.close()
        updateweb = updateWebsiteQuery()
except Exception as er:
    print(er)


# Get new status quo for this
# Get status quo
NewSQEquipmentQuery ="""
SELECT asset.name AS asset, asset.id AS assetID
FROM asset
WHERE asset.manufacturer_client_id LIKE '%{0}%'
""".format(manufacturer_id)

def getnewSQEquipment():
    db_connection_sales_mgr = mysql.connector.connect(**dbconfig)
    SQnewequipment = pd.read_sql(NewSQEquipmentQuery, db_connection_sales_mgr)
    return SQnewequipment

SQnewequip = getnewSQEquipment()

print("These numbers below should match exactly...")
print("Old ID Rows: " + str(oldrows))
print("New ID Rows: " + str(len(SQnewequip.index)))
