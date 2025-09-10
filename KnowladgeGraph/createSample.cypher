// Load Teams

LOAD CSV ///teams.csv' AS row
WITH HEADERS FROM 'file:
CREATE (:Team { id: row.id, name: row.team_name });

// Load Players

LOAD CSV ///players.csv' AS row
WITH HEADERS FROM 'file:
MATCH (t:Team { id: row.team_id })
CREATE (:Player { id: row.id, name: row.player_name })-[:PLAYS_FOR]->(t);

// Load Venues

LOAD CSV ///venues.csv' AS row
WITH HEADERS FROM 'file:
CREATE (:Venue { id: row.id, name: row.venue_name, city: row.city });

// Load Matches

LOAD CSV ///matches.csv' AS row
WITH HEADERS FROM 'file:
MATCH (t1:Team { id: row.team1_id }), (t2:Team {id: row.team2_id}), (v:Venue {id: row.venue_id})
CREATE (m:
MATCH { id: row.id, date: row.date })
CREATE (m)-[:TEAM1]->(t1)
CREATE (m)-[:TEAM2]->(t2)
CREATE (m)-[:PLAYED_AT]->(v);
