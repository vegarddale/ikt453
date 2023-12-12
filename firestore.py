from flask import render_template, request
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# Use a service account.
cred = credentials.Certificate(
    "./creds/intelligent-data-mngt-firebase-adminsdk-aw9xp-91e3a22110.json"
)

app = firebase_admin.initialize_app(cred)

db = firestore.client()


def firestore_queries():
    # Query 1-5
    docs = list(db.collection("fact_table").stream())

    regions = set([doc.to_dict()["region_txt"] for doc in docs])

    years = set([doc.to_dict()["iyear"] for doc in docs])

    targets = set([doc.to_dict()["targtype1_txt"] for doc in docs])

    attack_types = set([doc.to_dict()["attacktype1_txt"] for doc in docs])

    gnames = set([doc.to_dict()["gname"] for doc in docs])

    target_sub_types = set([doc.to_dict()["targsubtype1_txt"] for doc in docs])

    countries = set([doc.to_dict()["country_txt"] for doc in docs])

    weapdetails = set([doc.to_dict()["weapdetail"] for doc in docs])

    db_sources = set([doc.to_dict()["dbsource"] for doc in docs])

    damage_dones = set([doc.to_dict()["propextent_txt"] for doc in docs])

    # Query 6
    docs_q6 = list(db.collection("city_gname").stream())

    gnames_q6 = set([doc.to_dict()["gname"] for doc in docs_q6])

    cities = set([doc.to_dict()["city"] for doc in docs_q6])

    # Query 7
    docs_q7 = list(db.collection("country_gname").stream())

    countries_q7 = set([doc.to_dict()["country_txt"] for doc in docs_q7])

    # Query 8
    docs_q8 = list(db.collection("region_gname").stream())

    regions_q8 = set([doc.to_dict()["region_txt"] for doc in docs_q8])

    query1_dicts = []
    query2_dicts = []
    query3_dicts = []
    query4_dicts = []
    query5_dicts = []
    query6_dicts = []
    query7_dicts = []
    query8_dicts = []

    # Query 1
    selected_target_type = request.form.get("target")
    selected_region = request.form.get("region")
    selected_start_year = request.form.get("start_year")
    selected_end_year = request.form.get("end_year")

    if (
        selected_target_type
        and selected_region
        and selected_start_year
        and selected_end_year
    ):
        query1 = (
            db.collection("fact_table")
            .where(filter=FieldFilter("targtype1_txt", "==", str(selected_target_type)))
            .where(filter=FieldFilter("region_txt", "==", str(selected_region)))
            .where(filter=FieldFilter("iyear", ">=", int(selected_start_year)))
            .where(filter=FieldFilter("iyear", "<=", int(selected_end_year)))
        )

        query1_dicts = [doc.to_dict() for doc in query1.stream()]

    # Query 2
    selected_region_q2 = request.form.get("region_q2")
    selected_attack_type = request.form.get("attack")
    selected_years = request.form.get("years")

    if selected_region_q2 and selected_attack_type and selected_years:
        query2 = (
            db.collection("fact_table")
            .where(
                filter=FieldFilter("attacktype1_txt", "==", str(selected_attack_type))
            )
            .where(filter=FieldFilter("region_txt", "==", str(selected_region_q2)))
            .where(filter=FieldFilter("iyear", ">=", int(2023 - int(selected_years))))
        )

        query2_dicts = [doc.to_dict() for doc in query2.stream()]

    # Query 3
    selected_region_q3 = request.form.get("region_q3")
    selected_target_sub = request.form.get("target_sub_type")
    selected_group = request.form.get("group")

    if selected_region_q3 and selected_target_sub and selected_group:
        query3 = (
            db.collection("fact_table")
            .where(
                filter=FieldFilter("targsubtype1_txt", "==", str(selected_target_sub))
            )
            .where(filter=FieldFilter("region_txt", "==", str(selected_region_q3)))
            .where(filter=FieldFilter("gname", "==", str(selected_group)))
        )

        query3_dicts = [doc.to_dict() for doc in query3.stream()]

    # Query 4
    selected_country = request.form.get("country")
    selected_weap_detail = request.form.get("weap")
    selected_start_year_q4 = request.form.get("start_year_q4")
    selected_end_year_q4 = request.form.get("end_year_q4")
    if (
        selected_country
        and selected_weap_detail
        and selected_start_year_q4
        and selected_end_year_q4
    ):
        query4 = (
            db.collection("fact_table")
            .where(filter=FieldFilter("weapdetail", "==", str(selected_weap_detail)))
            .where(filter=FieldFilter("country_txt", "==", str(selected_country)))
            .where(filter=FieldFilter("iyear", ">=", int(selected_start_year_q4)))
            .where(filter=FieldFilter("iyear", "<=", int(selected_end_year_q4)))
        )

        query4_dicts = [doc.to_dict() for doc in query4.stream()]

    # Query 5
    selected_db_source = request.form.get("db_source")
    selected_damage_done = request.form.get("damage_done")

    if selected_db_source and selected_damage_done:
        query5 = (
            db.collection("fact_table")
            .where(filter=FieldFilter("dbsource", "==", str(selected_db_source)))
            .where(
                filter=FieldFilter("propextent_txt", "==", str(selected_damage_done))
            )
        )

        query5_dicts = [doc.to_dict() for doc in query5.stream()]

    # Query 6
    selected_city = request.form.get("city")
    selected_gname_q6 = request.form.get("gname_q6")

    if selected_gname_q6 and selected_city:
        query6 = (
            db.collection("city_gname")
            .where(filter=FieldFilter("gname", "==", str(selected_gname_q6)))
            .where(filter=FieldFilter("city", "==", str(selected_city)))
        )

        query6_dicts = [doc.to_dict() for doc in query6.stream()]

    # Query 7
    selected_country_q7 = request.form.get("country_q7")
    selected_gname_q7 = request.form.get("gname_q7")

    if selected_country_q7 and selected_gname_q7:
        query7 = (
            db.collection("country_gname")
            .where(filter=FieldFilter("country_txt", "==", str(selected_country_q7)))
            .where(filter=FieldFilter("gname", "==", str(selected_gname_q7)))
        )

        query7_dicts = [doc.to_dict() for doc in query7.stream()]

    # Query 8
    selected_region_q8 = request.form.get("region_q8")
    selected_gname_q8 = request.form.get("gname_q8")

    if selected_region_q8 and selected_gname_q8:
        query8 = (
            db.collection("region_gname")
            .where(filter=FieldFilter("region_txt", "==", str(selected_region_q8)))
            .where(filter=FieldFilter("gname", "==", str(selected_gname_q8)))
        )

        query8_dicts = [doc.to_dict() for doc in query8.stream()]

    return render_template(
        "firebase_data.html",
        regions=regions,
        start_years=years,
        end_years=years,
        target_types=targets,
        query1_dicts=query1_dicts,
        regions_q2=regions,
        attack_types=attack_types,
        query2_dicts=query2_dicts,
        regions_q3=regions,
        target_sub_types=target_sub_types,
        groups=gnames,
        query3_dicts=query3_dicts,
        countries=countries,
        weaps=weapdetails,
        start_years_q4=years,
        end_years_q4=years,
        query4_dicts=query4_dicts,
        db_sources=db_sources,
        damage_dones=damage_dones,
        query5_dicts=query5_dicts,
        cities=cities,
        gnames_q6=gnames_q6,
        query6_dicts=query6_dicts,
        countries_q7=countries_q7,
        gnames_q7=gnames_q6,
        query7_dicts=query7_dicts,
        regions_q8=regions_q8,
        gnames_q8=gnames_q6,
        query8_dicts=query8_dicts,
    )
