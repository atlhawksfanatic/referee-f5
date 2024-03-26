# Build the datasets -- well just pbp from nba dot com since 1996

# ---- start --------------------------------------------------------------

library(duckdb)
library(tidyverse)

local_dir <- "0-data/duckdb"
if (!dir.exists(local_dir)) dir.create(local_dir, recursive = T)

raw_dirs <- list.dirs("0-data/shufinskiy/raw",
                      full.names = T, recursive = F)

# Connect to or start the DB
duck_con <- dbConnect(
  duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
)

# ignore duckdb folders
filecon <- file(paste0(local_dir, "/.gitignore"))
writeLines("*duckdb*", filecon)
close(filecon)

# ---- functions ----------------------------------------------------------

# main is the table name....
upsert_db <- function(con, data, tbl_name) {
  # create an empty table matching tbl_name
  ct <- str_glue(
    "CREATE OR REPLACE TEMP TABLE stg as 
   SELECT * FROM {tbl_name} WHERE 1 = 2"
  )
  
  dbExecute(con, ct)
  dbAppendTable(con, "stg", data)
  
  # merge the data between the two tables
  iq <- str_glue(
    "INSERT INTO {tbl_name}
   SELECT * FROM stg;"
  )
  rr <- dbExecute(con, iq)
  
  # drop the source merge table
  dq <- "DROP TABLE stg"
  dbExecute(con, dq)
  rr
}


# ---- nba-pbp ------------------------------------------------------------


# Create the nbastats table
dbSendQuery(duck_con,
            "CREATE TABLE IF NOT EXISTS nbastats (
              PRIMARY KEY (game_id, eventnum),
              game_id VARCHAR,
              eventnum INTEGER,
              eventmsgtype INTEGER,
              eventmsgactiontype INTEGER,
              period INTEGER,
              wctimestring VARCHAR,
              pctimestring TIME,
              homedescription VARCHAR,
              neutraldescription VARCHAR,
              visitordescription VARCHAR,
              score VARCHAR,
              scoremargin VARCHAR,
              person1type INTEGER,
              player1_id INTEGER,
              player1_name VARCHAR,
              player1_team_id INTEGER,
              player1_team_city VARCHAR,
              player1_team_nickname VARCHAR,
              player1_team_abbreviation VARCHAR,
              person2type INTEGER,
              player2_id INTEGER,
              player2_name VARCHAR,
              player2_team_id INTEGER,
              player2_team_city VARCHAR,
              player2_team_nickname VARCHAR,
              player2_team_abbreviation VARCHAR,
              person3type INTEGER,
              player3_id INTEGER,
              player3_name VARCHAR,
              player3_team_id INTEGER,
              player3_team_city VARCHAR,
              player3_team_nickname VARCHAR,
              player3_team_abbreviation VARCHAR,
              video_available_flag INTEGER
            );")

nbastats_types <- cols(
  GAME_ID = "character",
  EVENTNUM = "numeric",
  EVENTMSGTYPE = "numeric",
  EVENTMSGACTIONTYPE = "numeric",
  PERIOD = "numeric",
  WCTIMESTRING = "character",
  PCTIMESTRING = "time",
  HOMEDESCRIPTION = "character",
  NEUTRALDESCRIPTION = "character",
  VISITORDESCRIPTION = "character",
  SCORE = "character",
  SCOREMARGIN = "character",
  PERSON1TYPE = "numeric",
  PLAYER1_ID = "numeric",
  PLAYER1_NAME = "character",
  PLAYER1_TEAM_ID = "numeric",
  PLAYER1_TEAM_CITY = "character",
  PLAYER1_TEAM_NICKNAME = "character",
  PLAYER1_TEAM_ABBREVIATION = "character",
  PERSON2TYPE = "numeric",
  PLAYER2_ID = "numeric",
  PLAYER2_NAME = "character",
  PLAYER2_TEAM_ID = "numeric",
  PLAYER2_TEAM_CITY = "character",
  PLAYER2_TEAM_NICKNAME = "character",
  PLAYER2_TEAM_ABBREVIATION = "character",
  PERSON3TYPE = "numeric",
  PLAYER3_ID = "numeric",
  PLAYER3_NAME = "character",
  PLAYER3_TEAM_ID = "numeric",
  PLAYER3_TEAM_CITY = "character",
  PLAYER3_TEAM_NICKNAME = "character",
  PLAYER3_TEAM_ABBREVIATION = "character",
  VIDEO_AVAILABLE_FLAG = "numeric"
)

nbastats_files <- dir("0-data/shufinskiy/raw/nbastats/",
                      pattern = "*.csv",
                      full.names = T)

existing_game_ids <- tbl(duck_con, "nbastats") |> 
  select(game_id) |>
  distinct() |>
  collect()

nbastats_files |> 
  map(function(x, topic = "nbastats") {
    print(x)
    temp_csv <- read_csv(x, col_types = nbastats_types) |> 
      mutate(GAME_ID = str_pad(GAME_ID, 10, side = "left", pad = "0")) |> 
      distinct() |> 
      rename_all(tolower)
    temp_csv |> 
      filter(!game_id %in% existing_game_ids$game_id) |> 
      upsert_db(con = duck_con,
                data = _,
                tbl_name = topic)
    gc()
  })

dbGetQuery(duck_con, "SELECT COUNT(*) FROM nbastats")
dbGetQuery(duck_con, "DESCRIBE nbastats")

# ---- insert-event-messages ----------------------------------------------

all_events <- tbl(duck_con, "nbastats") |> 
  select(eventmsgtype, eventmsgactiontype) |> 
  distinct() |> 
  collect()

eventmsgtype_cross <- c("1" = "made_shot",
                        "2" = "missed_shot",
                        "3" = "free_throw",
                        "4" = "rebound",
                        "5" = "turnover",
                        "6" = "foul",
                        "7" = "violation",
                        "8" = "substitution",
                        "9" = "timeout",
                        "10" = "jumpball",
                        "11" = "ejection",
                        "12" = "start_period",
                        "13" = "end_period",
                        "18" = "")

event_cross <- read_csv("0-data/internal/template_for_crosswalk.csv") |> 
  rename_all(tolower)

event_messages <- all_events |> 
  mutate(eventmsgtype_desc = eventmsgtype_cross[eventmsgtype]) |> 
  left_join(event_cross) |> 
  arrange(eventmsgtype, eventmsgactiontype)

write_csv(event_messages, file = "0-data/internal/crosswalk_to_do.csv")

dbSendQuery(duck_con, "DROP TABLE IF EXISTS event_messages;
            CREATE TABLE event_messages (
              PRIMARY KEY (eventmsgtype, eventmsgactiontype),
              eventmsgtype INTEGER,
              eventmsgtype_desc VARCHAR,
              eventmsgactiontype INTEGER,
              eventmsgactiontype_desc VARCHAR,
            );")
dbGetQuery(duck_con, "SELECT COUNT(*) FROM event_messages")

upsert_db(con = duck_con,
          data = event_messages,
          tbl_name = "event_messages")

dbGetQuery(duck_con, "SELECT COUNT(*) FROM event_messages")
dbGetQuery(duck_con, "DESCRIBE event_messages")


# ---- datanba ------------------------------------------------------------

# Create the datanba table
dbSendQuery(duck_con,
            "CREATE TABLE IF NOT EXISTS datanba (
            PRIMARY KEY (GAME_ID, evt),
            evt INTEGER,
            wallclk TIMESTAMP,
            cl TIME,
            de VARCHAR,
            locx INTEGER,
            locy INTEGER,
            opt1 INTEGER,
            opt2 INTEGER,
            opt3 INTEGER,
            opt4 INTEGER,
            mtype INTEGER,
            etype INTEGER,
            opid INTEGER,
            tid INTEGER,
            pid INTEGER,
            hs INTEGER,
            vs INTEGER,
            epid INTEGER,
            oftid INTEGER,
            period INTEGER,
            game_id VARCHAR,
            ord INTEGER,
            pts INTEGER
            );"
            )

datanba_types <- cols(
  evt = "d",
  wallclk = "T",
  cl = "t",
  de = "c",
  locX = "d",
  locY = "d",
  opt1 = "d",
  opt2 = "d",
  opt3 = "d",
  opt4 = "d",
  mtype = "d",
  etype = "d",
  opid = "d",
  tid = "d",
  pid = "d",
  hs = "d",
  vs = "d",
  epid = "d",
  oftid = "d",
  PERIOD = "d",
  GAME_ID = "c",
  ord = "d",
  pts = "d"
)

datanba_files <- dir("0-data/shufinskiy/raw/datanba/",
                     pattern = "*.csv",
                     full.names = T)

existing_game_ids <- tbl(duck_con, "datanba") |> 
  select(game_id) |>
  distinct() |>
  collect()

map(datanba_files, function(x, topic = "datanba") {
  print(x)
  temp_csv <- read_csv(x, col_types = datanba_types) |>
    mutate(GAME_ID = str_pad(GAME_ID, 10, side = "left", pad = "0")) |> 
    rename_all(tolower)
  
  temp_csv |> 
    filter(!game_id %in% existing_game_ids$game_id) |> 
    upsert_db(con = duck_con,
              data = _,
              tbl_name = topic)
  gc()
})


dbGetQuery(duck_con, "SELECT COUNT(*) FROM datanba")
dbGetQuery(duck_con, "DESCRIBE datanba")


# ---- datanba-msgs -------------------------------------------------------


datanba_events <- tbl(duck_con, "datanba") |> 
  select(etype, mtype) |> 
  distinct() |> 
  arrange(etype, mtype) |> 
  collect()

etype_cross <- c("1" = "made_shot",
                 "2" = "missed_shot",
                 "3" = "free_throw",
                 "4" = "rebound",
                 "5" = "turnover",
                 "6" = "foul",
                 "7" = "violation",
                 "8" = "substitution",
                 "9" = "timeout",
                 "10" = "jumpball",
                 "11" = "ejection",
                 "12" = "start_period",
                 "13" = "end_period",
                 "18" = "instant_replay",
                 "20" = "stoppage")

event_cross <- read_csv("0-data/internal/template_for_datanba.csv") |> 
  rename_all(tolower)

datanba_messages <- datanba_events |> 
  mutate(etype_desc = etype_cross[as.character(etype)]) |> 
  left_join(event_cross) |> 
  arrange(etype, mtype)

write_csv(datanba_messages,
          file = "0-data/internal/crosswalk_to_do_datanba.csv")

dbSendQuery(duck_con, "DROP TABLE IF EXISTS event_messages_datanba;
            CREATE TABLE event_messages_datanba (
              PRIMARY KEY (etype, mtype),
              etype INTEGER,
              etype_desc VARCHAR,
              mtype INTEGER,
              mtype_desc VARCHAR,
            );")
dbGetQuery(duck_con, "SELECT COUNT(*) FROM event_messages_datanba")

dbWriteTable(duck_con,
             name = "event_messages_datanba",
             value = datanba_messages,
             append = TRUE)

dbGetQuery(duck_con, "SELECT COUNT(*) FROM event_messages_datanba")
dbGetQuery(duck_con, "DESCRIBE event_messages_datanba")
