# Create the referee log

# Going into the play by play for events that would include a ref, let's
#  extract that action to assign a referee calling an eventnum

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

# ---- referee-foul-table -------------------------------------------------

dbListTables(duck_con)

foul_regex <- "\\(([[:print:]]+)\\).*\\(([[:print:]]+)\\)"
neutral_regex <- str_glue("{foul_regex}?.*\\(([[:print:]]+)\\)")

# Get messages where a ref was potentially involved in a foul call
potential_fouls <- tbl(duck_con, from = "nbastats") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  left_join(tbl(duck_con, "ref_box")) |> 
  left_join(tbl(duck_con, "event_messages")) |> 
  filter(season > 2014) |>
  filter(grepl(foul_regex, homedescription) |
           grepl(foul_regex, neutraldescription) |
           grepl(foul_regex, visitordescription),
         eventmsgtype == 6) |> 
  collect()

identify_fouls <- potential_fouls |>
  select(game_id, contains("event"), contains("description"),
         official_1, official_2, official_3, official_4) |> 
  pivot_longer(cols = c(homedescription, visitordescription,
                        neutraldescription),
               names_to = "var", values_to = "val") |> 
  filter(!is.na(val)) |> 
  mutate(
    ref = ifelse(var == "neutraldescription",
                 str_extract(val, neutral_regex, group = 3),
                 str_extract(val, foul_regex, group = 2)),
    ref_match = str_replace(ref, "\\s", "") |> 
      str_to_upper(),
    ref_last = str_extract(ref, "[^.]+$")
    )

foul_crosswalk <- identify_fouls |> 
  select(game_id, official_1, official_2, official_3, official_4) |> 
  distinct() |> 
  pivot_longer(-game_id,
               names_to = "position",
               values_to = "official") |> 
  filter(!is.na(official)) |> 
  mutate(ref_match = str_replace(official,
                                 "([A-Z]?)([[:graph:]]*) (.*)",
                                 "\\1.\\3") |> 
           str_replace("\\s", "") |> 
           str_to_upper())

ref_fouls <- identify_fouls |> 
  left_join(foul_crosswalk) |> 
  select(game_id, contains("event"),
         position, official)

foul_vars <- sapply(ref_fouls, class) |> 
  enframe() |> 
  mutate(duck_type = duck_types_cross[value])

# Make the duckdb sql code for variable types
ref_duck_vars <- glue::glue_collapse(
  glue::glue("{foul_vars$name} {foul_vars$duck_type}"),
  ", ")

create_ref_foul <- glue::glue("DROP TABLE IF EXISTS ref_fouls;
                              CREATE TABLE ref_fouls ({ref_duck_vars}, ",
                              "PRIMARY KEY(game_id, eventnum))")

dbSendQuery(duck_con, create_ref_foul)

# Write data to table
dbWriteTable(duck_con, "ref_fouls", ref_fouls,
             append = TRUE, row.names = FALSE)

dbGetQuery(duck_con, "SELECT COUNT(*) FROM ref_fouls")
dbGetQuery(duck_con, "DESCRIBE ref_fouls")


# ---- referee-violation-table --------------------------------------------

violation_regex <- "\\(([[:print:]]+)\\)"
# Get messages where a ref was potentially involved in a foul call
potential_violations <- tbl(duck_con, from = "nbastats") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  left_join(tbl(duck_con, "ref_box")) |> 
  left_join(tbl(duck_con, "event_messages")) |> 
  filter(season > 2014) |>
  filter(grepl(violation_regex, homedescription) |
           grepl(violation_regex, neutraldescription) |
           grepl(violation_regex, visitordescription),
         eventmsgtype == 7) |> 
  collect()

identify_violation <- potential_violations |>
  select(game_id, contains("event"), contains("description"),
         official_1, official_2, official_3, official_4) |> 
  pivot_longer(cols = c(homedescription, visitordescription,
                        neutraldescription),
               names_to = "var", values_to = "val") |> 
  filter(!is.na(val)) |> 
  mutate(
    ref = str_extract(val, violation_regex, group = 1),
    ref_match = str_replace(ref, "\\s", "") |> 
      str_to_upper(),
    ref_last = str_extract(ref, "[^.]+$")
  )

violation_crosswalk <- identify_violation |> 
  select(game_id, official_1, official_2, official_3, official_4) |> 
  distinct() |> 
  pivot_longer(-game_id,
               names_to = "position",
               values_to = "official") |> 
  filter(!is.na(official)) |> 
  mutate(ref_match = str_replace(official,
                                 "([A-Z]?)([[:graph:]]*) (.*)",
                                 "\\1.\\3") |> 
           str_replace("\\s", "") |> 
           str_to_upper())

ref_violations <- identify_violation |> 
  left_join(violation_crosswalk) |> 
  select(game_id, contains("event"),
         position, official)

violation_vars <- sapply(ref_violations, class) |> 
  enframe() |> 
  mutate(duck_type = duck_types_cross[value])

# Make the duckdb sql code for variable types
ref_duck_vars <- glue::glue_collapse(
  glue::glue("{violation_vars$name} {violation_vars$duck_type}"),
  ", ")

create_ref_violations <-
  glue::glue("DROP TABLE IF EXISTS ref_violations;
             CREATE TABLE ref_violations ({ref_duck_vars}, ",
             "PRIMARY KEY(game_id, eventnum))")

dbSendQuery(duck_con, create_ref_violations)

# Write data to table
dbWriteTable(duck_con, "ref_violations", ref_violations,
             append = TRUE, row.names = FALSE)

dbGetQuery(duck_con, "SELECT COUNT(*) FROM ref_violations")
dbGetQuery(duck_con, "DESCRIBE ref_violations")
