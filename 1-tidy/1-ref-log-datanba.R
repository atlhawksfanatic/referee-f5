# Create the referee log from datanba

# Going into the play by play for events that would include a ref, let's
#  extract that action to assign a referee calling an evt

# Current does not check for gameid+evt already in database for adding on new
#  observations (or being able to run this more than once)

# ---- start --------------------------------------------------------------

library(duckdb)
library(tidyverse)

local_dir <- "0-data/duckdb"
if (!dir.exists(local_dir)) dir.create(local_dir, recursive = T)

# Connect to or start the DB
duck_con <- dbConnect(
  # duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
  duckdb(dbdir = str_glue("{local_dir}/shufinskiy.duckdb"))
)

duck_types_cross <- c("character" = "VARCHAR",
                      "numeric" = "INTEGER",
                      "integer" = "INTEGER",
                      "Date" = "DATE")

foul_regex <- "\\(([[:print:]]+)\\).*\\(([[:print:]]+)\\)"
violation_regex <- "\\(([[:print:]]+)\\)"

# ---- referee-foul-table -------------------------------------------------

dbListTables(duck_con)

# dbSendQuery(duck_con, "drop table ref_fouls_datanba")
# dbSendQuery(duck_con, "drop table ref_violations_datanba")

szns <- tbl(duck_con, "datanba") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  select(season) |> 
  distinct() |> 
  collect()

szns |> 
  pull(season) |> 
  map(function(x) {
    print(x)
    # Get messages where a ref was potentially involved in a foul call
    potential_fouls <- tbl(duck_con, from = "datanba") |> 
      left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
      left_join(tbl(duck_con, "ref_box")) |> 
      left_join(tbl(duck_con, "event_messages_datanba")) |> 
      filter(season == x) |>
      filter(grepl(violation_regex, de),
             etype == 6) |> 
      collect()
    
    identify_fouls <- potential_fouls |>
      select(game_id, contains("type"), evt, de,
             official_1, official_2, official_3, official_4) |> 
      mutate(
        maybe_ref = str_extract(de, foul_regex, group = 2),
        reffy = str_extract(de, violation_regex, group = 1)
      ) |> 
      mutate(ref = case_when(maybe_ref == "Casey" ~ "G Petraitis",
                             grepl("Drell", maybe_ref) ~ "I Hwang",
                             grepl("[0-9]", maybe_ref) ~ NA_character_,
                             is.na(maybe_ref) ~ reffy,
                             T ~ maybe_ref),
             ref_match = ref |> 
               # Remove the second space if it exists
               str_replace("^(\\S+\\s+\\S+)\\s+", "\\1") |> 
               str_to_upper())
    
    foul_crosswalk <- identify_fouls |> 
      select(game_id, official_1, official_2, official_3, official_4) |> 
      distinct() |> 
      pivot_longer(-game_id,
                   names_to = "position",
                   values_to = "official") |> 
      filter(!is.na(official)) |> 
      mutate(ref_match = str_replace(official,
                                     "([A-Z]?)([[:graph:]]*) (.*)",
                                     "\\1 \\3") |> 
               str_replace("^(\\S+\\s+\\S+)\\s+", "\\1") |> 
               # str_replace("\\s", "") |> 
               str_to_upper())
    
    ref_fouls <- identify_fouls |> 
      left_join(foul_crosswalk) |> 
      select(game_id, evt,
             etype, etype_desc,
             mtype, mtype_desc,
             position, official, ref_match, de)
    
    foul_vars <- sapply(ref_fouls, class) |> 
      enframe() |> 
      mutate(duck_type = duck_types_cross[value])
    
    # Make the duckdb sql code for variable types
    ref_duck_vars <- glue::glue_collapse(
      glue::glue("{foul_vars$name} {foul_vars$duck_type}"),
      ", ")
    
    create_ref_foul <- glue::glue(
      "CREATE TABLE IF NOT EXISTS ref_fouls_datanba ({ref_duck_vars}, ",
      "PRIMARY KEY(game_id, evt))")
    
    dbSendQuery(duck_con, create_ref_foul)
    
    # Write data to table
    dbWriteTable(duck_con, "ref_fouls_datanba", ref_fouls,
                 append = TRUE, row.names = FALSE)
    gc()
  })

dbGetQuery(duck_con, "SELECT COUNT(*) FROM ref_fouls_datanba")
dbGetQuery(duck_con, "DESCRIBE ref_fouls_datanba")


# ---- referee-violation-table --------------------------------------------

szns |> 
  pull(season) |> 
  map(function(x) {
    print(x)
    # Get messages where a ref was potentially involved in a foul call
    potential_violations <- tbl(duck_con, from = "datanba") |> 
      left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
      left_join(tbl(duck_con, "ref_box")) |> 
      left_join(tbl(duck_con, "event_messages_datanba")) |> 
      filter(season == x) |>
      filter(grepl(violation_regex, de),
             etype == 7) |> 
      collect()
    
    identify_violation <- potential_violations |>
      select(game_id, contains("type"), evt, de,
             official_1, official_2, official_3, official_4) |> 
      mutate(
        maybe_ref = str_extract(de, violation_regex, group = 1)
      ) |> 
      mutate(ref = case_when(maybe_ref == "J VanDuyne" ~ "J Van Duyne",
                             maybe_ref == "Casey" ~ "G Petraitis",
                             grepl("Drell", maybe_ref) ~ "I Hwang",
                             grepl("[0-9]", maybe_ref) ~ NA_character_,
                             T ~ maybe_ref),
             ref_match = ref |> 
               str_replace("^(\\S+\\s+\\S+)\\s+", "\\1") |> 
               str_to_upper())
    
    violation_crosswalk <- identify_violation |> 
      select(game_id, official_1, official_2, official_3, official_4) |> 
      distinct() |> 
      pivot_longer(-game_id,
                   names_to = "position",
                   values_to = "official") |> 
      filter(!is.na(official)) |> 
      mutate(ref_match = str_replace(official,
                                     "([A-Z]?)([[:graph:]]*) (.*)",
                                     "\\1 \\3") |> 
               str_replace("^(\\S+\\s+\\S+)\\s+", "\\1") |> 
               # str_replace("\\s", "") |> 
               str_to_upper())
    
    ref_violations <- identify_violation |> 
      left_join(violation_crosswalk) |> 
      select(game_id, evt,
             etype, etype_desc,
             mtype, mtype_desc,
             position, official, ref_match, de)
    
    violation_vars <- sapply(ref_violations, class) |> 
      enframe() |> 
      mutate(duck_type = duck_types_cross[value])
    
    # Make the duckdb sql code for variable types
    ref_duck_vars <- glue::glue_collapse(
      glue::glue("{violation_vars$name} {violation_vars$duck_type}"),
      ", ")
    
    create_ref_violations <-
      glue::glue(
        "CREATE TABLE IF NOT EXISTS ref_violations_datanba ({ref_duck_vars}, ",
        "PRIMARY KEY(game_id, evt))")
    dbSendQuery(duck_con, create_ref_violations)
    
    # Write data to table
    dbWriteTable(duck_con, "ref_violations_datanba", ref_violations,
                 append = TRUE, row.names = FALSE)
    gc()
  })

dbGetQuery(duck_con, "SELECT COUNT(*) FROM ref_violations_datanba")
dbGetQuery(duck_con, "DESCRIBE ref_violations_datanba")

# ---- sanity -------------------------------------------------------------

na_officialz <- tbl(duck_con, "ref_fouls_datanba") |> 
  union_all(tbl(duck_con, "ref_violations_datanba")) |> 
  filter(is.na(official)) |> 
  collect()

na_officialz |> 
  group_by(ref_match) |> 
  tally() |> 
  arrange(n)

na_officialz |> 
  filter(is.na(ref_match))

na_officialz |> 
  filter(ref_match %in% c("T MADDOX", "B TAYLOR", "T BROTHERS")) |> 
  glimpse()

tbl(duck_con, "ref_box") |> 
  filter(game_id == "0042000133") |> 
  glimpse()
