import os
import json
import numpy as np
from google.cloud import bigquery


def getGdeltNewsPerQClass(moy,keyfile):
    client = bigquery.Client.from_service_account_json(keyfile)
    if not os.path.exists("CSV/{}.csv".format(moy)):
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = True

        sql ="SELECT " \
             "SOURCEURL, EventRootCode, NumMentions, QuadClass, ActionGeo_CountryCode, ActionGeo_ADM2Code, ActionGeo_Lat, ActionGeo_Long, SQLDATE,DATEADDED  " \
             "FROM `gdelt-bq.gdeltv2.events` " \
             "where MonthYear = {} and ActionGeo_ADM2Code IS NOT NULL;".format(moy)

        QClassNews = client.query(sql).to_dataframe()

        QClassNews.to_csv("CSV/{}.csv".format(moy))


if __name__ == '__main__':
    listkey=[]
    listdate = []
    for r, d, f in os.walk("keys"):
        for file in f:
            if file.endswith(".json"):
                listkey.append(os.path.join(r, file))

    while len(listkey) > 0:
        try:
            keyfile= listkey[0]
            for y in range (2000,2022):
                for m in range(1, 13):
                    moy = int("{}{}".format(str(y),str(m).zfill(2)))
                    listdate.append(moy)


            for moy in listdate[::-1]:
                print(moy)
                getGdeltNewsPerQClass(moy,keyfile)

        except:
            listkey.remove(keyfile)
            pass
