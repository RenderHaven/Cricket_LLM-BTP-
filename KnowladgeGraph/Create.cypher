// NOTE: The following script syntax is valid for database version 5.0 and above.

:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'players.csv',
  file_1: 'teams.csv',
  file_2: 'team_stat.csv',
  file_3: 'venues_cleaned.csv',
  file_4: 'match_info_refined.csv',
  file_5: 'player_partnership.csv',
  file_6: 'batter_vs_bowler.csv',
  file_7: 'player_performance.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `player_id_Players_uniq` IF NOT EXISTS
FOR (n: `Players`)
REQUIRE (n.`player_id`) IS UNIQUE;
CREATE CONSTRAINT `team_id_Teams_uniq` IF NOT EXISTS
FOR (n: `Teams`)
REQUIRE (n.`team_id`) IS UNIQUE;
CREATE CONSTRAINT `Id_Team_Statistics_uniq` IF NOT EXISTS
FOR (n: `Team Statistics`)
REQUIRE (n.`Id`) IS UNIQUE;
CREATE CONSTRAINT `venue_id_Venues_uniq` IF NOT EXISTS
FOR (n: `Venues`)
REQUIRE (n.`venue_id`) IS UNIQUE;
CREATE CONSTRAINT `match_id_Matches_uniq` IF NOT EXISTS
FOR (n: `Matches`)
REQUIRE (n.`match_id`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`player_id` IN $idsToSkip AND NOT toInteger(trim(row.`player_id`)) IS NULL
CALL (row) {
  MERGE (n: `Players` { `player_id`: toInteger(trim(row.`player_id`)) })
  SET n.`player_id` = toInteger(trim(row.`player_id`))
  SET n.`player_name` = row.`player_name`
  SET n.`team_id` = toInteger(trim(row.`team_id`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`team_id` IN $idsToSkip AND NOT toInteger(trim(row.`team_id`)) IS NULL
CALL (row) {
  MERGE (n: `Teams` { `team_id`: toInteger(trim(row.`team_id`)) })
  SET n.`team_id` = toInteger(trim(row.`team_id`))
  SET n.`team_name` = row.`team_name`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`id` IN $idsToSkip AND NOT toInteger(trim(row.`id`)) IS NULL
CALL (row) {
  MERGE (n: `Team Statistics` { `Id`: toInteger(trim(row.`id`)) })
  SET n.`Id` = toInteger(trim(row.`id`))
  SET n.`match_id` = toInteger(trim(row.`match_id`))
  SET n.`team_id` = toInteger(trim(row.`team_id`))
  SET n.`inning` = toInteger(trim(row.`inning`))
  SET n.`total_score` = toInteger(trim(row.`total_score`))
  SET n.`wickets` = toInteger(trim(row.`wickets`))
  SET n.`50_in_balls` = toInteger(trim(row.`50_in_balls`))
  SET n.`100_in_balls` = toInteger(trim(row.`100_in_balls`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`venue_id` IN $idsToSkip AND NOT toInteger(trim(row.`venue_id`)) IS NULL
CALL (row) {
  MERGE (n: `Venues` { `venue_id`: toInteger(trim(row.`venue_id`)) })
  SET n.`venue_id` = toInteger(trim(row.`venue_id`))
  SET n.`venue_name` = row.`venue_name`
  SET n.`city` = row.`city`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row
WHERE NOT row.`match_id` IN $idsToSkip AND NOT toInteger(trim(row.`match_id`)) IS NULL
CALL (row) {
  MERGE (n: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  SET n.`match_id` = toInteger(trim(row.`match_id`))
  SET n.`match_name` = row.`match_name`
  SET n.`match_type` = row.`match_type`
  SET n.`city` = row.`city`
  SET n.`venue_id` = toInteger(trim(row.`venue_id`))
  SET n.`venue` = row.`venue`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`date` = datetime(row.`date`)
  SET n.`team_1` = toInteger(trim(row.`team_1`))
  SET n.`team_2` = toInteger(trim(row.`team_2`))
  SET n.`toss_winner` = toInteger(trim(row.`toss_winner`))
  SET n.`toss_decision` = row.`toss_decision`
  SET n.`winner` = row.`winner`
  SET n.`result` = row.`result`
  SET n.`player_of_match` = toInteger(trim(row.`player_of_match`))
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Players` { `player_id`: toInteger(trim(row.`player_id`)) })
  MATCH (target: `Teams` { `team_id`: toInteger(trim(row.`team_id`)) })
  MERGE (source)-[r: `Playes For`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_5) AS row
WITH row 
CALL (row) {
  MATCH (source: `Players` { `player_id`: toInteger(trim(row.`player1_id`)) })
  MATCH (target: `Players` { `player_id`: toInteger(trim(row.`player2_id`)) })
  MERGE (source)-[r: `Patnership`]->(target)
  SET r.`match_id` = toInteger(trim(row.`match_id`))
  SET r.`player1_id` = toInteger(trim(row.`player1_id`))
  SET r.`player2_id` = toInteger(trim(row.`player2_id`))
  SET r.`team_id` = toInteger(trim(row.`team_id`))
  SET r.`runs_scored_in_inning` = toInteger(trim(row.`runs_scored_in_inning`))
  SET r.`balls_played_in_inning` = toInteger(trim(row.`balls_played_in_inning`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_6) AS row
WITH row 
CALL (row) {
  MATCH (source: `Players` { `player_id`: toInteger(trim(row.`batter_id`)) })
  MATCH (target: `Players` { `player_id`: toInteger(trim(row.`bowler_id`)) })
  MERGE (source)-[r: `Batter To Bowler`]->(target)
  SET r.`batter_team_id` = toInteger(trim(row.`batter_team_id`))
  SET r.`bowler_team_id` = toInteger(trim(row.`bowler_team_id`))
  SET r.`runs_scored_in_inning` = toInteger(trim(row.`runs_scored_in_inning`))
  SET r.`balls_played_in_inning` = toInteger(trim(row.`balls_played_in_inning`))
  SET r.`fours_in_inning` = toInteger(trim(row.`fours_in_inning`))
  SET r.`sixes_in_inning` = toInteger(trim(row.`sixes_in_inning`))
  SET r.`out` = toInteger(trim(row.`out`))
  SET r.`batter_id` = toInteger(trim(row.`batter_id`))
  SET r.`bowler_id` = toInteger(trim(row.`bowler_id`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL (row) {
  MATCH (source: `Teams` { `team_id`: toInteger(trim(row.`team_id`)) })
  MATCH (target: `Team Statistics` { `Id`: toInteger(trim(row.`id`)) })
  MERGE (source)-[r: `Statistics`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL (row) {
  MATCH (source: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  MATCH (target: `Venues` { `venue_id`: toInteger(trim(row.`venue_id`)) })
  MERGE (source)-[r: `Played At`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL (row) {
  MATCH (source: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  MATCH (target: `Players` { `player_id`: toInteger(trim(row.`player_of_match`)) })
  MERGE (source)-[r: `Player Of The Match`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL (row) {
  MATCH (source: `Teams` { `team_id`: toInteger(trim(row.`team_1`)) })
  MATCH (target: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  MERGE (source)-[r: `Matches`]->(target)
  SET r.`match_type` = row.`match_type`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  // SET r.`date` = datetime(row.`date`)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL (row) {
  MATCH (source: `Teams` { `team_id`: toInteger(trim(row.`team_2`)) })
  MATCH (target: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  MERGE (source)-[r: `Matches`]->(target)
  SET r.`match_type` = row.`match_type`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  // SET r.`date` = datetime(row.`date`)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_7) AS row
WITH row 
CALL (row) {
  MATCH (source: `Players` { `player_id`: toInteger(trim(row.`player_id`)) })
  MATCH (target: `Matches` { `match_id`: toInteger(trim(row.`match_id`)) })
  MERGE (source)-[r: `Player_Performance`]->(target)
  SET r.`match_type` = row.`match_type`
  SET r.`team_id` = toInteger(trim(row.`team_id`))
  SET r.`team_name` = row.`team_name`
  SET r.`opponent_team_id` = toInteger(trim(row.`opponent_team_id`))
  SET r.`runs_scored_in_inning` = toInteger(trim(row.`runs_scored_in_inning`))
  SET r.`balls_played_in_inning` = toInteger(trim(row.`balls_played_in_inning`))
  SET r.`fours_in_inning` = toInteger(trim(row.`fours_in_inning`))
  SET r.`sixes_in_inning` = toInteger(trim(row.`sixes_in_inning`))
  SET r.`batting_position` = toInteger(trim(row.`batting_position`))
  SET r.`50_in_balls` = toInteger(trim(row.`50_in_balls`))
  SET r.`100_in_balls` = toLower(trim(row.`100_in_balls`)) IN ['1','true','yes']
  SET r.`wicket_taken_in_inning` = toInteger(trim(row.`wicket_taken_in_inning`))
  SET r.`balls_bowled_in_inning` = toInteger(trim(row.`balls_bowled_in_inning`))
  SET r.`runs_conceded_in_inning` = toInteger(trim(row.`runs_conceded_in_inning`))
  SET r.`fours_conceded_in_inning` = toInteger(trim(row.`fours_conceded_in_inning`))
  SET r.`sixes_conceded_in_inning` = toInteger(trim(row.`sixes_conceded_in_inning`))
  SET r.`maiden_in_inning` = toLower(trim(row.`maiden_in_inning`)) IN ['1','true','yes']
  SET r.`economy` = toFloat(trim(row.`economy`))
  SET r.`is_out` = toLower(trim(row.`is_out`)) IN ['1','true','yes']
  SET r.`is_player_of_match` = toLower(trim(row.`is_player_of_match`)) IN ['1','true','yes']
} IN TRANSACTIONS OF 10000 ROWS;
