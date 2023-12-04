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
    # Connect to MySQL database
    # dw_conn = None
    # dw_load_query = "INSERT INTO fact(locid,prodid,sale) " "VALUES(%s,%s,%s)"

    consumer = KafkaConsumer(
        "AggrData", bootstrap_servers="localhost:29092", api_version=(2, 0, 2)
    )

    print("\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...")

    while True:
        for message in consumer:
            in_string = message.value.decode()

            in_split = in_string.split(",")

            eventid = in_split[0].strip(" '")
            year = in_split[1].strip(" '")
            month = in_split[2].strip(" '")

            in_tuple = (eventid, year, month)
            print("\nTuple Received: {}".format(in_tuple))

            try:
                updict = {"year": year, "month": month}
                db.collection("fact").document(eventid).set(updict)
                print("\nAGGREGATED TUPLES are uploaded to Firestore")
            except Error as e:
                print(e)


if __name__ == "__main__":
    dw_consumer()
