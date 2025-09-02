// Load Teams

LOAD CSV WITH HEADERS FROM 'file:///teams.csv' AS row
CREATE (:Team {id: row.id, name: row.team_name});

// Load Players

LOAD CSV WITH HEADERS FROM 'file:///players.csv' AS row
MATCH (t:Team {id: row.team_id})
CREATE (:Player {id: row.id, name: row.player_name})-[:PLAYS_FOR]->(t);

// Load Venues

LOAD CSV WITH HEADERS FROM 'file:///venues.csv' AS row
CREATE (:Venue {id: row.id, name: row.venue_name, city: row.city});

// Load Matches

LOAD CSV WITH HEADERS FROM 'file:///matches.csv' AS row
MATCH (t1:Team {id: row.team1_id}), (t2:Team {id: row.team2_id}), (v:Venue {id: row.venue_id})
CREATE (m:Match {id: row.id, date: row.date})
CREATE (m)-[:TEAM1]->(t1)
CREATE (m)-[:TEAM2]->(t2)
CREATE (m)-[:PLAYED_AT]->(v);