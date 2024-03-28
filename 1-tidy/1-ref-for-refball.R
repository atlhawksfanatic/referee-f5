# Tabulate Referee Calls for shiny app

# ---- start --------------------------------------------------------------

library(duckdb)
library(tidyverse)

local_dir <- "0-data/duckdb"
if (!dir.exists(local_dir)) dir.create(local_dir, recursive = T)

# Connect to or start the DB
duck_con <- dbConnect(
  duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
)

duck_types_cross <- c("character" = "VARCHAR",
                      "numeric" = "INTEGER",
                      "integer" = "INTEGER",
                      "Date" = "DATE")

dbListTables(duck_con)

# ---- calls --------------------------------------------------------------

ref_foul_calls <- tbl(duck_con, "ref_fouls") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official,
           eventmsgtype_desc, eventmsgactiontype_desc) |> 
  summarise(calls = n()) |> 
  collect()

ref_violation_calls <- tbl(duck_con, "ref_violations") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official,
           eventmsgtype_desc, eventmsgactiontype_desc) |> 
  summarise(calls = n()) |> 
  collect()

ref_games <- tbl(duck_con, "ref_fouls") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official) |> 
  summarise(total_games = n_distinct(game_id)) |> 
  collect()

# datanba
datanba_foul_calls <- tbl(duck_con, "ref_fouls_datanba") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official,
           etype_desc, mtype_desc) |> 
  summarise(calls = n()) |> 
  collect()

datanba_violation_calls <- tbl(duck_con, "ref_violations_datanba") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official,
           etype_desc, mtype_desc) |> 
  summarise(calls = n()) |> 
  collect()

datanba_games <- tbl(duck_con, "ref_fouls_datanba") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  filter(season > 2015) |> 
  group_by(season, season_prefix, official) |> 
  summarise(total_games = n_distinct(game_id)) |> 
  collect()



# ---- join ---------------------------------------------------------------

(ref_calls <- bind_rows(ref_foul_calls, ref_violation_calls) |> 
   pivot_wider(names_from = c(eventmsgtype_desc, eventmsgactiontype_desc),
               values_from = calls) |> 
   arrange(season, season_prefix, official) |> 
   left_join(ref_games) |> 
   mutate(total_calls = rowSums(across(matches("foul|violation")),
                                na.rm = T)))

(datanba_calls <- bind_rows(datanba_foul_calls, datanba_violation_calls) |> 
    pivot_wider(names_from = c(etype_desc, mtype_desc),
                values_from = calls) |> 
    arrange(season, season_prefix, official) |> 
    left_join(datanba_games) |> 
    mutate(total_calls = rowSums(across(matches("foul|violation")),
                                 na.rm = T)))

# ---- select-vars --------------------------------------------------------


shiny_ref_vars <- c(Referee = "official",
                    Games	= "total_games",
                    "Total fouls" = "total_calls",
                    "Shooting" = "foul_shooting",
                    "Personal" = "foul_personal",
                    "Loose ball" = "foul_loose_ball",
                    "Personal take" = "foul_personal_take",
                    "Offensive charge" = "foul_offensive_charge",
                    Offensive = "foul_offensive",
                    "Kicked ball" = "violation_kicked_ball",
                    Technical ="foul_technical",
                    "Defensive goaltending" =
                      "violation_defensive_goaltending",
                    "Shooting block" = "foul_shooting_block",
                    "Defensive 3 seconds" = "foul_defensive_three_second",
                    "Delay of game" = "violation_delay_of_game",
                    "Personal block" = "foul_personal_block",
                    "Flagrant 1" = "foul_flagrant_one",
                    "Away from play" = "foul_away_from_play",
                    "Double technical" = "foul_double_technical",
                    "Clear path" = "foul_clear_path",
                    "Double personal" = "foul_double_personal",
                    "Inbound foul" = "foul_inbound",
                    "Delay technical" = "foul_delay_technical",
                    "Lane violation" = "violation_lane_violation",
                    "Jump ball violation" = "violation_jump_ball_violation",
                    "Flagrant 2" = "foul_flagrant_two",
                    "Hanging technical" = "foul_hanging_technical",
                    "Unsportsmanlike technical" =
                      "foul_unsportsmanlike_technical",
                    "Taunting technical" = "foul_taunting_technical",
                    "Excess timeout technical" =
                      "foul_excess_timeout_technical",
                    "Double lane" = "violation_double_lane",
                    "Too many players technical" =
                      "foul_too_many_players_technical")

datanba_calls |> 
  select(season, season_prefix,
         shiny_ref_vars) |> 
  mutate(across(everything(), ~replace_na(.x, 0))) |> 
  write_csv("2-shiny/refball/datanba_szn_calls.csv")
