from flask import Flask, render_template, request, jsonify
from firestore import firestore_queries
from relational_sql import relational_sql_queries
from transfer_data import render_transfer_data
from insert_data import render_insert_data
from neo4j import GraphDatabase

app = Flask("GTDB")


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/relational_sql", methods=["GET", "POST"])
def relational_sql_route():
    return relational_sql_queries()


@app.route("/firestore", methods=["GET", "POST"])
def firestore_route():
    return firestore_queries()


@app.route("/transfer_data", methods=["GET", "POST"])
def transfer_data_route():
    return render_transfer_data()


@app.route("/insert_data")
def insert_data_route():
    return render_insert_data()


uri = 'neo4j://192.168.10.163:7687'
user = 'neo4j'
password = 'Password123.'

def execute_neo4j_query(query):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            result = session.run(query)
            return result.data() 

@app.route('/neo4j', methods=["GET", "POST"])
def run_query():
    if request.method == 'POST':
        query_id = request.form.get('query_number')
        region_txt = request.form.get('region_txt')
        targtype1_txt = request.form.get('targtype1_txt')
        start_year = request.form.get('start_year')
        end_year = request.form.get('end_year')
        attack_type = request.form.get('attack_type')
        country_txt = request.form.get('country_txt')
        gname_id = request.form.get('country_txt')
        propextent_txt = request.form.get('propextent_txt')
        city_id = request.form.get('city_id')
        queries = {
            "Q1": (
                f"MATCH (q1:summary_table_q1)-[:Q1_TARGET]->(target:target_dim), "
                f"(region:region_dim) "
                f"WHERE q1.region_id = region.region_id "
                f"AND region.region_txt = '{region_txt}' "
                f"AND target.targtype1_txt = '{targtype1_txt}' "
                f"AND q1.year >= {start_year} AND q1.year <= {end_year} "
                f"RETURN SUM(q1.total_nof_incidents) AS total_incidents;"
            ),
            "Q2": (
                f"MATCH (q2:summary_table_q2), "
                f"(city:city_dim)-[:BELONGS_TO_COUNTRY]->(country:country_dim)-[:BELONGS_TO_REGION]->(region:region_dim), "
                f"(q2)-[:Q2_ATTACKTYPE]->(attacktype:attack_type_dim) "
                f"WHERE attacktype.attacktype1_txt = '{attack_type}' "
                f"AND region.region_txt = '{region_txt}' "
                f"AND q2.year >= datetime().year - 100 "
                f"RETURN q2.city_id as city_id, SUM(q2.total_nof_incidents) AS total_incidents "
                f"ORDER BY total_incidents DESC;"
            ),
            "Q4": (
                f"MATCH (q4:summary_table_q4), "
                f"(attacktype:attack_type_dim), "
                f"(country:country_dim) "
                f"WHERE q4.attacktype_id = attacktype.attacktype_id "
                f"AND q4.country_id = country.country_id "
                f"AND attacktype.attacktype1_txt='Bombing/Explosion' "
                f"AND country.country_txt = 'Turkey' "
                f"AND q4.year >= 1970 AND q4.year <= 1971 "
                f"RETURN q4;"
            ),
            "Q5": (
                f"MATCH(q5:summary_table_q5)-[:Q5_KILLS_DAMAGE_INFO]->(pe:propextent_dim) "
                f"WHERE q5.gname_id='White supremacists/nationalists' "
                f"AND pe.propextent_txt = 'Minor (likely < $1 million)' "
                f"RETURN SUM(q5.total_nof_incidents) as total_incidents, q5.gname_id as gname_id;"
            ),
            "Q6": (
                f"MATCH(kills:kills_fact) "
                f"WHERE kills.gname_id='Black Nationalists' "
                f"AND kills.city_id='Cairo' "
                f"RETURN kills.gname_id as gname_id, kills.city_id as city_id, SUM(kills.nof_incidents) as total_incidents, "
                f"SUM(kills.nof_kills) as total_kills;"
            ),
            "Q7": (
                f"MATCH (q7:summary_table_q7)-[:Q7_OCURRED_IN_COUNTRY]->(country:country_dim) "
                f"WHERE country.country_txt='United States' "
                f"AND q7.gname_id='Black Panthers' "
                f"RETURN country.country_txt as country_txt, q7.gname_id as gname_id, SUM(q7.total_nof_incidents) as total_incidents, "
                f"SUM(q7.total_nof_kills) as total_kills;"
            ),
            "Q8": (
                f"MATCH (q8:summary_table_q8)-[:Q8_OCURRED_IN_REGION]->(region:region_dim) "
                f"WHERE region.region_txt='Central America & Caribbean' "
                f"AND q8.gname_id='Montoneros (Argentina)' "
                f"RETURN region.region_txt as region_txt, q8.gname_id as gname_id, SUM(q8.total_nof_incidents) as total_incidents, "
                f"SUM(q8.total_nof_kills) as total_kills;"
            ),
        }
        
        result_data = execute_neo4j_query(queries.get(query_id, ""))
        print(result_data)
        print(query_id)
        return render_template('results.html', result_data=result_data, query_id=query_id)
    
    return render_template('results.html', query_id=None)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
