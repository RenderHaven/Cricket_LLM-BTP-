// ==========================
// CONSTRAINTS
// ==========================

CREATE CONSTRAINT player_id_Players_uniq IF NOT EXISTS
FOR (n:Players)
REQUIRE n.player_id IS UNIQUE;

CREATE CONSTRAINT team_id_Teams_uniq IF NOT EXISTS
FOR (n:Teams)
REQUIRE n.team_id IS UNIQUE;

CREATE CONSTRAINT Id_Team_Statistics_uniq IF NOT EXISTS
FOR (n:`Team Statistics`)
REQUIRE n.Id IS UNIQUE;

CREATE CONSTRAINT venue_id_Venues_uniq IF NOT EXISTS
FOR (n:Venues)
REQUIRE n.venue_id IS UNIQUE;

CREATE CONSTRAINT match_id_Matches_uniq IF NOT EXISTS
FOR (n:Matches)
REQUIRE n.match_id IS UNIQUE;


// ==========================
// NODES
// ==========================

LOAD CSV WITH HEADERS FROM 'file:///players.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.player_id)) IS NULL
CALL {
  WITH row
  MERGE (n:Players {player_id: toInteger(trim(row.player_id))})
  SET n.player_name = row.player_name,
      n.team_id = toInteger(trim(row.team_id))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///teams.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.team_id)) IS NULL
CALL {
  WITH row
  MERGE (n:Teams {team_id: toInteger(trim(row.team_id))})
  SET n.team_name = row.team_name
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///team_stat.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.id)) IS NULL
CALL {
  WITH row
  MERGE (n:`Team Statistics` {Id: toInteger(trim(row.id))})
  SET n.match_id = toInteger(trim(row.match_id)),
      n.team_id = toInteger(trim(row.team_id)),
      n.inning = toInteger(trim(row.inning)),
      n.total_score = toInteger(trim(row.total_score)),
      n.wickets = toInteger(trim(row.wickets)),
      n.`50_in_balls` = toInteger(trim(row.`50_in_balls`)),
      n.`100_in_balls` = toInteger(trim(row.`100_in_balls`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///venues_cleaned.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.venue_id)) IS NULL
CALL {
  WITH row
  MERGE (n:Venues {venue_id: toInteger(trim(row.venue_id))})
  SET n.venue_name = row.venue_name,
      n.city = row.city
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///match_info_refined.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.match_id)) IS NULL
CALL {
  WITH row
  MERGE (n:Matches {match_id: toInteger(trim(row.match_id))})
  SET n.match_name = row.match_name,
      n.match_type = row.match_type,
      n.city = row.city,
      n.venue_id = toInteger(trim(row.venue_id)),
      n.venue = row.venue,
      n.date = datetime(row.date),   // ensure ISO 8601 format in CSV
      n.team_1 = toInteger(trim(row.team_1)),
      n.team_2 = toInteger(trim(row.team_2)),
      n.toss_winner = toInteger(trim(row.toss_winner)),
      n.toss_decision = row.toss_decision,
      n.winner = row.winner,
      n.result = row.result,
      n.player_of_match = toInteger(trim(row.player_of_match))
} IN TRANSACTIONS OF 10000 ROWS;


// ==========================
// RELATIONSHIPS
// ==========================

LOAD CSV WITH HEADERS FROM 'file:///players.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (p:Players {player_id: toInteger(trim(row.player_id))})
  MATCH (t:Teams {team_id: toInteger(trim(row.team_id))})
  MERGE (p)-[:`Plays For`]->(t)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///player_partnership.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (p1:Players {player_id: toInteger(trim(row.player1_id))})
  MATCH (p2:Players {player_id: toInteger(trim(row.player2_id))})
  MERGE (p1)-[r:Partnership]->(p2)
  SET r.match_id = toInteger(trim(row.match_id)),
      r.player1_id = toInteger(trim(row.player1_id)),
      r.player2_id = toInteger(trim(row.player2_id)),
      r.team_id = toInteger(trim(row.team_id)),
      r.runs_scored_in_inning = toInteger(trim(row.runs_scored_in_inning)),
      r.balls_played_in_inning = toInteger(trim(row.balls_played_in_inning))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///batter_vs_bowler.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (b:Players {player_id: toInteger(trim(row.batter_id))})
  MATCH (bo:Players {player_id: toInteger(trim(row.bowler_id))})
  MERGE (b)-[r:`Batter To Bowler`]->(bo)
  SET r.batter_team_id = toInteger(trim(row.batter_team_id)),
      r.bowler_team_id = toInteger(trim(row.bowler_team_id)),
      r.runs_scored_in_inning = toInteger(trim(row.runs_scored_in_inning)),
      r.balls_played_in_inning = toInteger(trim(row.balls_played_in_inning)),
      r.fours_in_inning = toInteger(trim(row.fours_in_inning)),
      r.sixes_in_inning = toInteger(trim(row.sixes_in_inning)),
      r.out = toInteger(trim(row.out)),
      r.batter_id = toInteger(trim(row.batter_id)),
      r.bowler_id = toInteger(trim(row.bowler_id))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///team_stat.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (t:Teams {team_id: toInteger(trim(row.team_id))})
  MATCH (ts:`Team Statistics` {Id: toInteger(trim(row.id))})
  MERGE (t)-[:Statistics]->(ts)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///match_info_refined.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (m:Matches {match_id: toInteger(trim(row.match_id))})
  MATCH (v:Venues {venue_id: toInteger(trim(row.venue_id))})
  MERGE (m)-[:`Played At`]->(v)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///match_info_refined.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (m:Matches {match_id: toInteger(trim(row.match_id))})
  MATCH (p:Players {player_id: toInteger(trim(row.player_of_match))})
  MERGE (m)-[:`Player Of The Match`]->(p)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///match_info_refined.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (t:Teams {team_id: toInteger(trim(row.team_1))})
  MATCH (m:Matches {match_id: toInteger(trim(row.match_id))})
  MERGE (t)-[r:Matches]->(m)
  SET r.match_type = row.match_type
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///match_info_refined.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (t:Teams {team_id: toInteger(trim(row.team_2))})
  MATCH (m:Matches {match_id: toInteger(trim(row.match_id))})
  MERGE (t)-[r:Matches]->(m)
  SET r.match_type = row.match_type
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///player_performance.csv' AS row
WITH row
CALL {
  WITH row
  MATCH (p:Players {player_id: toInteger(trim(row.player_id))})
  MATCH (m:Matches {match_id: toInteger(trim(row.match_id))})
  MERGE (p)-[r:Player_Performance]->(m)
  SET r.match_type = row.match_type,
      r.team_id = toInteger(trim(row.team_id)),
      r.team_name = row.team_name,
      r.opponent_team_id = toInteger(trim(row.opponent_team_id)),
      r.runs_scored_in_inning = toInteger(trim(row.runs_scored_in_inning)),
      r.balls_played_in_inning = toInteger(trim(row.balls_played_in_inning)),
      r.fours_in_inning = toInteger(trim(row.fours_in_inning)),
      r.sixes_in_inning = toInteger(trim(row.sixes_in_inning)),
      r.batting_position = toInteger(trim(row.batting_position)),
      r.`50_in_balls` = toInteger(trim(row.`50_in_balls`)),
      r.`100_in_balls` = toLower(trim(row.`100_in_balls`)) IN ['1','true','yes'],
      r.wicket_taken_in_inning = toInteger(trim(row.wicket_taken_in_inning)),
      r.balls_bowled_in_inning = toInteger(trim(row.balls_bowled_in_inning)),
      r.runs_conceded_in_inning = toInteger(trim(row.runs_conceded_in_inning)),
      r.fours_conceded_in_inning = toInteger(trim(row.fours_conceded_in_inning)),
      r.sixes_conceded_in_inning = toInteger(trim(row.sixes_conceded_in_inning)),
      r.maiden_in_inning = toLower(trim(row.maiden_in_inning)) IN ['1','true','yes'],
      r.economy = toFloat(trim(row.economy)),
      r.is_out = toLower(trim(row.is_out)) IN ['1','true','yes'],
      r.is_player_of_match = toLower(trim(row.is_player_of_match)) IN ['1','true','yes']
} IN TRANSACTIONS OF 10000 ROWS;
