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
  duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
)

# ---- query --------------------------------------------------------------

dbListTables(duck_con)

# duck_games <- tbl(duck_con, from = "nbastats")

ref_regex <- "\\(([[:print:]]+)\\).*\\(([[:print:]]+)\\)"
neutral_regex <- str_glue("{ref_regex}?.*\\(([[:print:]]+)\\)")

# Get messages where a ref was potentially involved in a foul call
potential_fouls <- tbl(duck_con, from = "nbastats") |> 
  left_join(tbl(duck_con, "game_ids"), by = c("game_id" = "gid")) |> 
  left_join(tbl(duck_con, "ref_box")) |> 
  left_join(tbl(duck_con, "event_messages")) |> 
  filter(season > 2014) |>
  filter(grepl(ref_regex, homedescription) |
           grepl(ref_regex, neutraldescription) |
           grepl(ref_regex, visitordescription),
         eventmsgtype == 6) |> 
  collect()

identify_ref <- potential_fouls |>
  select(game_id, contains("event"), contains("description"),
         official_1, official_2, official_3, official_4) |> 
  pivot_longer(cols = c(homedescription, visitordescription,
                        neutraldescription),
               names_to = "var", values_to = "val") |> 
  filter(!is.na(val)) |> 
  mutate(
    ref = ifelse(var == "neutraldescription",
                 str_extract(val, neutral_regex, group = 3),
                 str_extract(val, ref_regex, group = 2)),
    ref_match = str_replace(ref, "([A-Z]?)([[:graph:]]*) (.*)", "\\1.\\3") |> 
      str_replace("\\s", "") |> 
      str_to_upper(),
    ref_last = str_extract(ref, "[^.]+$")
    )

ref_crosswalk <- identify_ref |> 
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

temp_ref <- identify_ref |> 
  left_join(ref_crosswalk) 

# Looks like an issue with last names going on...
temp_ref |> 
  write_csv("1-tidy/temp_ref.csv")
