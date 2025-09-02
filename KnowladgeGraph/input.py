import requests, json
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test123"))
def query_kg(cypher):
    with driver.session() as session:
        result = session.run(cypher)
        return [record.data() for record in result]
kg_res = query_kg("MATCH (p:Person)-[:WORKS_AT]->(c:Company {name:'OpenAI'}) RETURN p.name")
prompt = f"Who works at OpenAI? The KG says: {kg_res}"
response = requests.post("http://localhost:11434/api/generate",
                         json={"model": "llama2", "prompt": prompt},
                         stream=True)
for line in response.iter_lines():
    if line:
        print(json.loads(line.decode())["response"], end="")
