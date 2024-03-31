# Bring in referee related data from L2M repo

# ---- start --------------------------------------------------------------

library(duckdb)
library(tidyverse)

local_dir <- "0-data/duckdb"
if (!dir.exists(local_dir)) dir.create(local_dir, recursive = T)

raw_dirs <- list.dirs("0-data/raw", full.names = T, recursive = F)

# Connect to or start the DB
duck_con <- dbConnect(
  # duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
  duckdb(dbdir = str_glue("{local_dir}/shufinskiy.duckdb"))
)

duck_types_cross <- c("character" = "VARCHAR",
                      "numeric" = "INTEGER",
                      "Date" = "DATE")

# ---- game-ids -----------------------------------------------------------

gameid_url <- str_glue("https://raw.githubusercontent.com/",
                       "atlhawksfanatic/L2M/master/0-data/",
                       "stats_nba/nba_game_schedule.csv")
game_ids <- read_csv(gameid_url) |> 
  mutate(season_prefix = substr(gid, 1, 3),
         season_start = substr(gid, 4, 5),
         season = ifelse(as.numeric(season_start) < 90,
                         2001 + as.numeric(season_start),
                         1901 + as.numeric(season_start))) |> 
  select(-season_start)

gameid_vars <- sapply(game_ids, class) |> 
  enframe() |> 
  mutate(duck_type = duck_types_cross[value])

# Make the duckdb sql code for variable types
gameid_duck_vars <- glue::glue_collapse(
  glue::glue("{gameid_vars$name} {gameid_vars$duck_type}"),
  ", ")

create_gameid <- glue::glue("DROP TABLE IF EXISTS game_ids;
                            CREATE TABLE game_ids ({gameid_duck_vars}, ",
                            "PRIMARY KEY(gid, gcode))")

dbSendQuery(duck_con, create_gameid)

# Write data to table
dbWriteTable(duck_con, "game_ids", game_ids,
             append = TRUE, row.names = FALSE)

dbGetQuery(duck_con, "SELECT COUNT(*) FROM game_ids")
dbGetQuery(duck_con, "DESCRIBE game_ids")


# ---- ref-assign ---------------------------------------------------------

# insert in the referee assignments
ref_assign_url <- str_glue("https://raw.githubusercontent.com/",
                           "atlhawksfanatic/L2M/master/0-data/",
                           "official_nba/nba_referee_assignments.csv")

ref_assign <- read_csv(ref_assign_url) |> 
  select(-season, -game_id) |> 
  mutate(game_date = as.Date(game_date, format = "%m/%d/%Y"))

ref_vars <- sapply(ref_assign, class) |> 
  enframe() |> 
  mutate(duck_type = duck_types_cross[value])

# Make the duckdb sql code for variable types
ref_duck_vars <- glue::glue_collapse(
  glue::glue("{ref_vars$name} {ref_vars$duck_type}"),
  ", ")

create_ref <- glue::glue("DROP TABLE IF EXISTS referee_assignments;
                         CREATE TABLE referee_assignments (",
                         "{ref_duck_vars}, ",
                         "PRIMARY KEY(game_code))")

dbSendQuery(duck_con, create_ref)

# Write data to table
dbWriteTable(duck_con, "referee_assignments", ref_assign,
             append = TRUE, row.names = FALSE)

dbGetQuery(duck_con, "SELECT COUNT(*) FROM referee_assignments")
dbGetQuery(duck_con, "DESCRIBE referee_assignments")


# ---- ref-box ------------------------------------------------------------

# insert in the referees from the box score, this might be available local
lcl_ref_box <- "0-data/duckdb/stats_nba/ref_box.csv"
if (file.exists(lcl_ref_box)) {
  ref_box <- read_csv(lcl_ref_box) |> 
    # Hack for 0011600107
    filter(game_id != "0011600107")
  
  ref_vars <- sapply(ref_box, class) |> 
    enframe() |> 
    mutate(duck_type = duck_types_cross[value])
  
  # Make the duckdb sql code for variable types
  ref_duck_vars <- glue::glue_collapse(
    glue::glue("{ref_vars$name} {ref_vars$duck_type}"),
    ", ")
  
  create_ref <- glue::glue("DROP TABLE IF EXISTS ref_box;
                         CREATE TABLE ref_box (",
                           "{ref_duck_vars}, ",
                           "PRIMARY KEY(game_id))")
  
  dbSendQuery(duck_con, create_ref)
  
  # Write data to table
  dbWriteTable(duck_con, "ref_box", ref_box,
               append = TRUE, row.names = FALSE)
  
  dbGetQuery(duck_con, "SELECT COUNT(*) FROM ref_box")
  dbGetQuery(duck_con, "DESCRIBE ref_box")
}



