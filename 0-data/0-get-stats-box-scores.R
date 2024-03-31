# Get Box Score Information with Officials Listed

# ---- start --------------------------------------------------------------

library(duckdb)
library(httr2)
library(tidyverse)

local_dir <- "0-data/duckdb"
stats_dir <- str_glue("{local_dir}/stats_nba")
stats_src <- str_glue("{stats_dir}/raw")
if (!dir.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!dir.exists(stats_src)) dir.create(stats_src, recursive = T)

# ignore raw folders
filecon <- file(paste0(stats_dir, "/.gitignore"))
writeLines("raw", filecon)
close(filecon)

# Connect to or start the DB
duck_con <- dbConnect(
  # duckdb(dbdir = str_glue("{local_dir}/ref5.duckdb"))
  duckdb(dbdir = str_glue("{local_dir}/shufinskiy.duckdb"))
)

duck_types_cross <- c("character" = "VARCHAR",
                      "numeric" = "INTEGER",
                      "Date" = "DATE")

# ---- create -----------------------------------------------------------

create_ref_box <-
  paste0("CREATE TABLE IF NOT EXISTS ref_box (
         game_date VARCHAR,
         attendance INTEGER,
         game_time VARCHAR,
         official_1 VARCHAR,
         official_2 VARCHAR,
         official_3 VARCHAR,
         official_4 VARCHAR,
         official_id_1 INTEGER,
         official_id_2 INTEGER,
         official_id_3 INTEGER,
         official_id_4 INTEGER,
         jersey_num_1 INTEGER,
         jersey_num_2 INTEGER,
         jersey_num_3 INTEGER,
         jersey_num_4 INTEGER,
         game_id VARCHAR,
         PRIMARY KEY(game_id)
         )")

dbSendQuery(duck_con, create_ref_box)

# ---- get-missing --------------------------------------------------------


# Only download the missing box scores:
missing_ids <- tbl(duck_con, "game_ids") |> 
  left_join(tbl(duck_con, "ref_box"), by = c("gid" = "game_id")) |> 
  filter(is.na(official_1), gid != 0042100407,
         date < today()) |> 
  # select(gid) |> 
  collect()


# ---- nba-stats-api ------------------------------------------------------

# Set up headers to make requests to API
stats_nba_headers <- c(
  "Host" = "stats.nba.com",
  # "User-Agent" = "libcurl/7.68.0 r-curl/4.3.2 httr/1.4.2",
  "User-Agent" = paste0("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) ",
                        "AppleWebKit/537.36 (KHTML, like Gecko) ",
                        "Chrome/79.0.3945.130 Safari/537.36"),
  "Accept" = "application/json, text/plain, */*",
  "Accept-Language" = "en-US,en;q=0.5",
  "Accept-Encoding" = "gzip, deflate, br",
  "x-nba-stats-origin" = "stats",
  "x-nba-stats-token" = "true",
  "Connection" = "keep-alive",
  "Referer" = "https =//stats.nba.com/",
  "Pragma" = "no-cache",
  "Cache-Control" = "no-cache"
)

# ---- query --------------------------------------------------------------

# Take the list of nba_game_ids that are missing box scores, then query API
#  for information on players, referees, and attendance

info_mapped <- missing_ids |> 
  filter(row_number() < 500) |> 
  pluck("gid") |>
  map(function(x) {
    # Sys.sleep(runif(1, 0.5, 2.5))
    print(paste(x, "at", Sys.time()))
    
    # Info on officials and attendance for a game
    x_url <- paste0("https://stats.nba.com/stats/boxscoresummaryv2?GameID=", x)
    
    res <- httr2::request(x_url) |> 
      httr2::req_headers(!!!stats_nba_headers) |> 
      httr2::req_throttle(rate = 30 / 60) |> 
      httr2::req_perform()
    
    # Exit out if the query is invalid
    if (httr2::resp_status(res) != 200) {
      return(NULL)
    } else {
      json <- httr2::resp_body_json(res, simplifyVector = T)
      
      json_map <- pmap(json$resultSets, function(name, headers, rowSet) {
        results <- data.frame(rowSet)
        # Sometimes this is empty, so ignore it if it is empty
        if (!length(results)) {
          results <- data.frame(table = name)
          return(results)
        }
        
        names(results) <- headers
        results$table <- name
        return(results)
      })
      
      game_info <- json_map |> 
        bind_rows() |> 
        filter(table == "GameInfo") |> 
        select_if(~ !any(is.na(.))) |> 
        select(-table)
      
      officials <- json_map |> 
        bind_rows() |> 
        filter(table == "Officials") |> 
        select_if(~ !any(is.na(.))) |> 
        select(-table)
      
      if (is_empty(officials)) {
        officials_wide <- data.frame(OFFICIAL_1 = NA)
      } else {
        officials_wide <- officials |> 
          arrange(OFFICIAL_ID) |> 
          mutate(OFFICIAL = paste(FIRST_NAME, LAST_NAME),
                 num = 1:n()) |> 
          select(-FIRST_NAME, -LAST_NAME) |> 
          pivot_wider(names_from = num,
                      values_from = c(OFFICIAL, OFFICIAL_ID, JERSEY_NUM),
                      names_glue = "{.value}_{num}")
        
      }
      
      game_output <- bind_cols(game_info, officials_wide) |> 
        mutate(GAME_ID = json$parameters$GameID)
      
      return(game_output)
    }
  })

# Now add this to the database
info_mapped |> 
  bind_rows() |>
  rename_all(tolower) |>
  filter(game_id %in% missing_ids$gid) |>
  mutate(across(contains(c("official_id",
                           "jersey_num",
                           "attendance")), as.numeric)) |> 
  dbAppendTable(conn = duck_con,
                name = "ref_box",
                value = _)

tbl(duck_con, "ref_box") |> 
  collect() |> 
  write_csv(str_glue("{stats_dir}/ref_box.csv"))

dbDisconnect(duck_con, shutdown = TRUE)
gc()

# box_mapped <- purrr::map(missing_ids$gid, function(x) {
#   Sys.sleep(runif(1, 0.5, 2.5))
#   print(paste(x, "at", Sys.time()))
#   
#   # Box Score Info:
#   # EndPeriod=1&EndRange=0&GameID=0021700807&
#   # RangeType=0&StartPeriod=1&StartRange=0
#   # x = "0022100747"
#   x_url <- paste0("https://stats.nba.com/stats/boxscoretraditionalv2?",
#                   "EndPeriod=1&EndRange=0&GameID=",
#                   x,
#                   "&RangeType=0&StartPeriod=1&StartRange=0")
#   
#   res <- httr2::request(x_url) |> 
#     httr2::req_headers(!!!stats_nba_headers) |> 
#     httr2::req_perform()
#   res <- GET(x_url, add_headers(stats_nba_headers))
#   
#   # Exit out if the query is invalid
#   if (httr2::resp_status(res) != 200) {
#     box_score_info <- data.frame(table = NA)
#     return(box_score_info)
#   } else {
#     json <- httr2::resp_body_json(res, simplifyVector = T)
#     
#     json_map <- pmap(json$resultSets, function(name, headers, rowSet) {
#       results <- data.frame(rowSet)
#       names(results) <- headers
#       results$table <- name
#       return(results)
#     })
#     
#     box_score_info <- json_map |> 
#       bind_rows() |> 
#       filter(table == "PlayerStats") |> 
#       select(any_of(c("GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION",
#                       "PLAYER_ID", "PLAYER_NAME", "NICKNAME", "MIN")))
#     
#     return(box_score_info)
#   }
# })
