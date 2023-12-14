from kafka import KafkaConsumer
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# Use a service account.
cred = credentials.Certificate(
    "./creds/intelligent-data-mngt-firebase-adminsdk-aw9xp-91e3a22110.json"
)

app = firebase_admin.initialize_app(cred)

db = firestore.client()


def dw_consumer():
    consumer = KafkaConsumer(
        "AggrData", bootstrap_servers="localhost:29092", api_version=(2, 0, 2)
    )

    print("\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...")

    while True:
        for message in consumer:
            in_string = message.value.decode()

            in_split = in_string.split(",")

            eventid = in_split[0].strip(" '")
            year = int(in_split[1].strip(" '"))
            month = int(in_split[2].strip(" '"))
            day = int(in_split[3].strip(" '"))
            country_id = int(in_split[4].strip(" '"))
            country_name = in_split[5].strip(" '")
            region_id = int(in_split[6].strip(" '"))
            region_name = in_split[7].strip(" '")
            provstate = in_split[8].strip(" '")
            city = in_split[9].strip(" '")
            location = in_split[10].strip(" '")
            attack_type_id = int(in_split[13].strip(" '"))
            attack_type = in_split[14].strip(" '")
            targ_type_id = int(in_split[15].strip(" '"))
            targ_type = in_split[16].strip(" '")
            targ_sub_type_id = int(in_split[17].strip(" '"))
            targ_sub_type = in_split[18].strip(" '")
            corp = in_split[19].strip(" '")
            target = in_split[20].strip(" '")
            natlty_id = in_split[21].strip(" '")
            natlty = in_split[22].strip(" '")
            gname = in_split[23].strip(" '")
            motive = in_split[24].strip(" '")
            weaptype_detail = in_split[25].strip(" '")
            propextent = in_split[27].strip(" '")
            propvalue = in_split[28].strip(" '")
            addnotes = in_split[29].strip(" '")
            scite1 = in_split[30].strip(" '")
            scite2 = in_split[31].strip(" '")
            scite3 = in_split[32].strip(" '")
            dbsource = in_split[33].strip(" '")

            in_tuple = (
                eventid,
                year,
                month,
                day,
                country_id,
                country_name,
                region_id,
                region_name,
                provstate,
                city,
                location,
                attack_type_id,
                attack_type,
                targ_type_id,
                targ_type,
                targ_sub_type_id,
                targ_sub_type,
                corp,
                target,
                natlty_id,
                natlty,
                gname,
                motive,
                weaptype_detail,
                propextent,
                propvalue,
                addnotes,
                scite1,
                scite2,
                scite3,
                dbsource,
            )
            print("\nTuple Received: {}".format(in_tuple))

            try:
                updict = {
                    "year": year,
                    "month": month,
                    "day": day,
                    "country_id": country_id,
                    "country_name": country_name,
                    "region_id": region_id,
                    "region_name": region_name,
                    "provstate": provstate,
                    "city": city,
                    "location": location,
                    "attack_type_id": attack_type_id,
                    "attack_type": attack_type,
                    "targ_type_id": targ_type_id,
                    "targ_type": targ_type,
                    "targ_sub_type_id": targ_sub_type_id,
                    "targ_sub_type": targ_sub_type,
                    "corp": corp,
                    "target": target,
                    "natlty_id": natlty_id,
                    "natlty": natlty,
                    "gname": gname,
                    "motive": motive,
                    "weaptype_detail": weaptype_detail,
                    "propextent": propextent,
                    "propvalue": propvalue,
                    "addnotes": addnotes,
                    "scite1": scite1,
                    "scite2": scite2,
                    "scite3": scite3,
                    "dbsource": dbsource,
                }
                db.collection("fact").document(eventid).set(updict)
                print("\nAGGREGATED TUPLES are uploaded to Firestore")
            except OSError as e:
                print(e)


if __name__ == "__main__":
    dw_consumer()
