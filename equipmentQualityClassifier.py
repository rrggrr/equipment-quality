import keyring, time, sys, string # PEP, meh.
import pandas as pd
import numpy as np
import nltk
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
import mysql.connector
from mysql.connector import MySQLConnection, Error
from getCredentials import gc

# Replace *** with osx keychain context
sys.path.append(keyring.get_password("pvtpath", "***"))
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
SELECT
asset_detailed.id as ASSET_ID,
media.size as MSIZE,
media.media_type as MTYPE,
asset_detailed.name as NAME,
asset_detailed.description as DESCRIPTION,
CONCAT(media.size,'\n', asset_detailed.name, '\n' ,asset_detailed.description, '\n' ,asset_detailed.internal_id, '\n', media.media_type) AS CDESCRIP,
asset_detailed.cost_asis as COST,
asset_detailed.sell_asis as SELL,
asset_detailed.listing_quality AS QUALITY,
COUNT(quote_asset_detailed.id) AS QCOUNT,
CHAR_LENGTH(asset_detailed.description) AS DESCRIPTION_LEN,
DATEDIFF(now(),asset_detailed.date_created) as CURRENT_AGE
FROM asset_detailed, quote_asset_detailed, media
WHERE quote_asset_detailed.asset_id = asset_detailed.id
AND media.asset_id = asset_detailed.id
AND asset_detailed.status = "available"
AND (media_type = 'image' or media_type = 'doc')
GROUP BY asset_detailed.id
ORDER BY COUNT(quote_asset_detailed.id)
"""

def getEquipment():
    db_connection_sales_mgr = mysql.connector.connect(**dbconfig)
    equipment = pd.read_sql(equipmentQuery, db_connection_sales_mgr)
    return equipment

####### run query and get assets for training and classification
equip = getEquipment()
####### training dataframe settings
descriptions = list(equip['CDESCRIP'])
labels = list(equip['QUALITY'])
aids = list(equip['ASSET_ID'])
names = list(equip['NAME'])
####### define the training classifier - more rice = more power.
training_size = int(len(descriptions) * 0.90)
train_descriptions = descriptions[:training_size]
train_labels = labels[:training_size]
####### define the testing classifier
test_descriptions = descriptions[training_size:]
test_labels = labels[training_size:]
####### tokenize dataframe content
# bag of words tokenize
vectorizer = CountVectorizer()
#tokenize asset descriptions
vectorizer.fit(train_descriptions)
# vectorize asset descriptions
train_features = vectorizer.transform(train_descriptions)
####### init classifier and train it
classifier = MultinomialNB()
classifier.fit(train_features.toarray(), train_labels)
####### test classifier
# Anything above 70% is acceptable threshold for accuracy for this task.
test_features = vectorizer.transform(test_descriptions)
classifier_accuracy = classifier.score(test_features, test_labels)
# Are we accurate?
print(classifier_accuracy)
if classifier_accuracy < .7:
    #print(classifier_accuracy)
    raise ValueError('Classifier Accuracy Below .7 - Aborting')
###### create empty dictionary for updating the database key=id, value=prediction
quality_data = {}
###### run classifier against our data and update data
run_description = descriptions
run_labels = labels
run_aids = aids
run_names = names
# classify and push to quality_data dictionary for updating
for d in range(int(len(run_description))):
    this_description = run_description[d]
    this_label = run_labels[d]
    this_aid = run_aids[d]
    this_features = vectorizer.transform([this_description])
    this_prediction = classifier.predict(this_features)[0]
    this_name = run_names[d]
    quality_data[this_aid] = (this_aid,this_prediction)

###### Transform dictionary to dataframe to make update easier for me.
df_quality = pd.DataFrame.from_dict(data=quality_data,orient='index')
df_quality = df_quality.rename(columns={0 : 'assetid'})
df_quality = df_quality.rename(columns={1 : 'prediction'})
# Split into individual dataframes by quality so I don't have to expand my mind further.
dfq_1 = df_quality.loc[df_quality['prediction'] == 1]
dfq_2 = df_quality.loc[df_quality['prediction'] == 2]
dfq_3 = df_quality.loc[df_quality['prediction'] == 3]
###### Begin insert process
# Quality update query
def querybuilder(assetid,qualitycode):

    dbconfig = {
        "database": cred.get("myd"),
        "user": cred.get("mydu"),
        "password": cred.get("mydp"),
        "use_unicode": "True",
        "buffered":"True",
    }
    # Howz it going?
    print("Updating Asset: ",assetid)
    print("Quality:",qualitycode)

    # Update quality based on classification
    updateQuality="""
    UPDATE asset
    SET listing_quality = {0}
    WHERE id = {1}
    """.format(qualitycode,assetid)

    # Must update our website queue
    fillQueue="""
    INSERT INTO website_api_sync_queue (entity_type, entity_id)
    VALUES ('assets', {1})""".format(0,assetid)

    def updateQualityQuery():
        cnx = mysql.connector.connect(**dbconfig)
        cursor = cnx.cursor()
        cursor.execute(updateQuality)
        cnx.commit()
        cursor.close()
        cnx.close()
    # Send it
    updateq = updateQualityQuery()

    def updateWebsiteQuery():
        cnx = mysql.connector.connect(**dbconfig)
        cursor = cnx.cursor()
        cursor.execute(fillQueue)
        cnx.commit()
        cursor.close()
        cnx.close()

    # Send it
    updatweb = updateWebsiteQuery()

###### Separate sends by quality 1,2,3 becuase I'm running out of time to get this pushed.

try:
    # Iterate through dataframe of quality results and send to database(s)
    for index, row in dfq_1.iterrows():
        i = row[0] #asset id
        q = row[1] #asset quality
        print("trying: ", i, q)
        qb = querybuilder(i,q)
        time.sleep(5)
except Exception as er:
    print(er)

# Give server a break because its fragile.
time.sleep(120)

try:
    for index, row in dfq_2.iterrows():
        i = row[0] #asset id
        q = row[1] #asset quality
        print("trying: ", i, q)
        qb = querybuilder(i,q)
        time.sleep(5)
except Exception as er:
    print(er)

# Give server a break because its fragile.
time.sleep(120)

try:
    for index, row in dfq_3.iterrows():
        i = row[0] #asset id
        q = row[1] #asset quality
        print("trying: ", i, q)
        qb = querybuilder(i,q)
        time.sleep(5)
except Exception as er:
    print(er)
