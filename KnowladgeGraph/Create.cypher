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

// Players
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/players.csv' AS row
WITH HEADERS FROM 'https:
WITH row
WHERE row.player_id IS NOT null AND row.player_id <> ''
MERGE (n:Players { player_id: toInteger(trim(row.player_id)) })
 SET n.player_name = row.player_name,
n.team_id =
CASE WHEN row.team_id <> '' THEN toInteger(trim(row.team_id)) ELSE null END;

// Teams
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/teams.csv' AS row
WITH HEADERS FROM 'https:
WITH row
WHERE row.team_id IS NOT null AND row.team_id <> ''
MERGE (n:Teams { team_id: toInteger(trim(row.team_id)) })
 SET n.team_name = row.team_name;

// Team Statistics
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/team_stat.csv' AS row
WITH HEADERS FROM 'https:
WITH row
WHERE row.id IS NOT null AND row.id <> ''
MERGE (n:`Team Statistics` { Id: toInteger(trim(row.id)) })
 SET n.match_id = toInteger(trim(row.match_id)),
n.team_id = toInteger(trim(row.team_id)),
n.inning = toInteger(trim(row.inning)),
n.total_score = toInteger(trim(row.total_score)),
n.wickets = toInteger(trim(row.wickets)),
n.`50_in_balls` = toInteger(trim(row.`50_in_balls`)),
n.`100_in_balls` = toInteger(trim(row.`100_in_balls`));

// Venues
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/venues_cleaned.csv' AS row
WITH HEADERS FROM 'https:
WITH row
WHERE row.venue_id IS NOT null AND row.venue_id <> ''
MERGE (n:Venues { venue_id: toInteger(trim(row.venue_id)) })
 SET n.venue_name = row.venue_name,
n.city = row.city;

// Matches
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/match_info.csv' AS row
WITH HEADERS FROM 'https:
WITH row
WHERE row.match_id IS NOT null AND row.match_id <> ''
MERGE (n:Matches { match_id: toInteger(trim(row.match_id)) })
 SET n.match_name = row.match_name,
n.match_type = row.match_type,
n.city = row.city,
n.venue_id =
CASE WHEN row.venue_id <> '' THEN toInteger(trim(row.venue_id)) ELSE null END,
n.venue = row.venue,
n.date = datetime(row.date), // must be ISO 8601 format
n.team_1 =
CASE WHEN row.team_1 <> '' THEN toInteger(trim(row.team_1)) ELSE null END,
n.team_2 =
CASE WHEN row.team_2 <> '' THEN toInteger(trim(row.team_2)) ELSE null END,
n.toss_winner =
CASE WHEN row.toss_winner <> '' THEN toInteger(trim(row.toss_winner)) ELSE null END,
n.toss_decision = row.toss_decision,
n.winner = row.winner,
n.result = row.result,
n.player_of_match =
CASE WHEN row.player_of_match <> '' THEN toInteger(trim(row.player_of_match)) ELSE null END;

// ==========================
// RELATIONSHIPS
// ==========================

// Player -> Team
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/players.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (p:Players { player_id: toInteger(trim(row.player_id)) })
MATCH (t:Teams { team_id: toInteger(trim(row.team_id)) })
MERGE (p)-[:`Plays For`]->(t);

// Player Partnerships
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/player_partnership.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (p1:Players { player_id: toInteger(trim(row.player1_id)) })
MATCH (p2:Players { player_id: toInteger(trim(row.player2_id)) })
MERGE (p1)-[r:Partnership]->(p2)
 SET r.match_id = toInteger(trim(row.match_id)),
r.player1_id = toInteger(trim(row.player1_id)),
r.player2_id = toInteger(trim(row.player2_id)),
r.team_id = toInteger(trim(row.team_id)),
r.runs_scored_in_inning = toInteger(trim(row.runs_scored_in_inning)),
r.balls_played_in_inning = toInteger(trim(row.balls_played_in_inning));

// Batter vs Bowler
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/batter_vs_bowler.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (b:Players { player_id: toInteger(trim(row.batter_id)) })
MATCH (bo:Players { player_id: toInteger(trim(row.bowler_id)) })
MERGE (b)-[r:`Batter To Bowler`]->(bo)
 SET r.batter_team_id = toInteger(trim(row.batter_team_id)),
r.bowler_team_id = toInteger(trim(row.bowler_team_id)),
r.runs_scored_in_inning = toInteger(trim(row.runs_scored_in_inning)),
r.balls_played_in_inning= toInteger(trim(row.balls_played_in_inning)),
r.fours_in_inning = toInteger(trim(row.fours_in_inning)),
r.sixes_in_inning = toInteger(trim(row.sixes_in_inning)),
r.out = toInteger(trim(row.out)),
r.batter_id = toInteger(trim(row.batter_id)),
r.bowler_id = toInteger(trim(row.bowler_id));

// Team -> Team Statistics
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/team_stat.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (t:Teams { team_id: toInteger(trim(row.team_id)) })
MATCH (ts:`Team Statistics` { Id: toInteger(trim(row.id)) })
MERGE (t)-[:Statistics]->(ts);

// Match -> Venue
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/match_info.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (m:Matches { match_id: toInteger(trim(row.match_id)) })
MATCH (v:Venues { venue_id: toInteger(trim(row.venue_id)) })
MERGE (m)-[:`Played At`]->(v);

// Match -> Player of Match
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/match_info.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (m:Matches { match_id: toInteger(trim(row.match_id)) })
MATCH (p:Players { player_id: toInteger(trim(row.player_of_match)) })
MERGE (m)-[:`Player Of The Match`]->(p);

// Team_1 -> Match
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/match_info.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (t:Teams { team_id: toInteger(trim(row.team_1)) })
MATCH (m:Matches { match_id: toInteger(trim(row.match_id)) })
MERGE (t)-[r:Matches]->(m)
 SET r.match_type = row.match_type;

// Team_2 -> Match
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/match_info.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (t:Teams { team_id: toInteger(trim(row.team_2)) })
MATCH (m:Matches { match_id: toInteger(trim(row.match_id)) })
MERGE (t)-[r:Matches]->(m)
 SET r.match_type = row.match_type;

// Player Performance
LOAD CSV //raw.githubusercontent.com/RenderHaven/Cricket_LLM-BTP-/main/KnowladgeGraph/InputData/player_performance.csv' AS row
WITH HEADERS FROM 'https:
WITH row
MATCH (p:Players { player_id: toInteger(trim(row.player_id)) })
MATCH (m:Matches { match_id: toInteger(trim(row.match_id)) })
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
r.`100_in_balls` = toLower(trim(row.`100_in_balls`)) IN ['1', 'true', 'yes'],
r.wicket_taken_in_inning = toInteger(trim(row.wicket_taken_in_inning)),
r.balls_bowled_in_inning = toInteger(trim(row.balls_bowled_in_inning)),
r.runs_conceded_in_inning = toInteger(trim(row.runs_conceded_in_inning)),
r.fours_conceded_in_inning= toInteger(trim(row.fours_conceded_in_inning)),
r.sixes_conceded_in_inning= toInteger(trim(row.sixes_conceded_in_inning)),
r.maiden_in_inning = toLower(trim(row.maiden_in_inning)) IN ['1', 'true', 'yes'],
r.economy = toFloat(trim(row.economy)),
r.is_out = toLower(trim(row.is_out)) IN ['1', 'true', 'yes'],
r.is_player_of_match = toLower(trim(row.is_player_of_match)) IN ['1', 'true', 'yes'];
