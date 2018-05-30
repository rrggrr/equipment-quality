import keyring, time, sys, string # PEP, meh.
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import MySQLConnection, Error
from getCredentials import gc

# Set manufacturer name
MFG = "PRAB"
MFGID = 15357

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

# Construct via CONCAT string for classification based on attributes that contribute quality listing.
equipmentQuery ="""
SELECT asset.name AS asset, asset.id AS assetID
FROM asset
WHERE asset.name LIKE '%{0}%'
""".format(MFG)

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
""".format(0,MFGID,assetIDString)

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
