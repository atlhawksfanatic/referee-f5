# 0-stats-nba-game-ids.R

# ---- start --------------------------------------------------------------

library(httr2)
library(lubridate)
library(rvest)
library(tidyverse)
library(duckdb)

local_dir   <- "0-data/stats_nba"
data_source <- paste0(local_dir, "/raw")
id_source   <- paste0(data_source, "/game_ids")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source, recursive = T)
if (!file.exists(id_source)) dir.create(id_source, recursive = T)

# ignore the downloaded info...
filecon <- file(paste0(local_dir, "/.gitignore"))
writeLines("raw", filecon)
close(filecon)

# Connect to or start the DB
duck_con <- dbConnect(
  duckdb(dbdir = str_glue("0-data/duckdb/ref5.duckdb"))
)

# ---- nba-stats-api ------------------------------------------------------

# Set up headers to make requests to API
stats_nba_headers <- c(
  "Host" = "stats.nba.com",
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

# ---- pre-2015-game-ids ---------------------------------------------------

# https://github.com/gmf05/nba
# The NBA's Game ID, 0021400001, is a 10-digit code: XXXYYGGGGG, where:
#  XXX refers to a season prefix,
#  YY is the season year (e.g. 14 for 2014-15),
#  and GGGGG refers to the game number (1-1230 for 30-team regular season).
# 
# Season prefixes are:# 
# 001 : Pre Season
# 002 : Regular Season
# 003 : All-Star
# 004 : Post Season
# 005 : Play-In
# https://raw.githubusercontent.com/gmf05/nba/master/data/csv/games_96-14.csv

# Pre 2015-16 gameids, note no scores are present.
if (file.exists(paste0(id_source, "/game_ids_pre2015_scores.csv"))) {
  pre2015_ids <- read_csv(paste0(id_source, "/game_ids_pre2015_scores.csv"))
} else {
  # Make the franchise names consistent for pre-2015, bad on left good on right
  team_cross <- c(#"ATL" = "",
    # "BKN" = "",
    # "BOS" = "",
    # "CHA" = "",
    "CHH" = "NOP",
    # "CHI" = "",
    # "CLE" = "",
    # "DAL" = "",
    # "DEN" = "",
    # "DET" = "",
    # "GSW" = "",
    # "HOU" = "",
    # "IND" = "",
    # "LAC" = "",
    # "LAL" = "",
    # "MEM" = "",
    # "MIA" = "",
    # "MIL" = "",
    # "MIN" = "",
    "NJN" = "BKN",
    "NOH" = "NOP",
    "NOK" = "NOP",
    # "NOP" = "",
    # "NYK" = "",
    # "OKC" = "",
    # "ORL" = "",
    # "PHI" = "",
    # "PHX" = "",
    # "POR" = "",
    # "SAC" = "",
    # "SAS" = "",
    "SEA" = "OKC",
    # "TOR" = "",
    # "UTA" = "",
    # "WAS" = "",
    "VAN" = "MEM")
  
  pre2015_ids <- paste0("https://raw.githubusercontent.com/gmf05/",
                        "nba/master/data/csv/games_96-14.csv") |> 
    read_csv(col_types = cols(.default = "c")) |> 
    janitor::clean_names()
  
  pre2015_ids <- pre2015_ids |> 
    mutate(date = as.Date(str_sub(game_code, 1, 8), format = "%Y%m%d"),
           home = ifelse(is.na(team_cross[home]),
                         home,
                         team_cross[home]),
           away = ifelse(is.na(team_cross[away]),
                         away,
                         team_cross[away])) |> 
    select(gid = game_id, gcode = game_code, date, home, away)
  
  write_csv(pre2015_ids, paste0(id_source, "/game_ids_pre2015.csv"))
}

# These are missing the 2015 Playoffs, need to add those in by guessing the
#  range of values that the game id can take on
# start: 0041400101
# end:   0041400406

yoffs_id_range <- str_pad(seq(0041400101, 0041400406), 10, "left", "0")

# ---- queries ------------------------------------------------------------

yoffs_2015_file <- paste0(local_dir, "/game_ids_2015_playoffs.csv")

# The 2015 Playoffs Query
if (file.exists(yoffs_2015_file)) {
  yoffs <- read_csv(yoffs_2015_file)
} else {
  yoffs_mapped <- purrr::map(yoffs_id_range, function(x) {
    # Sys.sleep(runif(1, 5, 15))
    print(paste(x, "at", Sys.time()))
    
    # Info on officials and attendance for a game
    x_url <- paste0("https://stats.nba.com/stats/boxscoresummaryv2?GameID=", x)
    
    
    res <- request(x_url) |> 
      req_headers(!!!stats_nba_headers) |> 
      req_error(is_error = \(resp) FALSE) |>
      req_throttle(rate = 15 / 60) |> 
      req_perform()
    
    # Exit out if the query is invalid
    if (resp_status(res) > 400) {
      id_info <- data.frame(gid = x)
      print(paste("Game ", x, " does not exist"))
      return(id_info)
    } else {
      json <- resp_body_json(res, simplifyVector = T)
      
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
      }) |> 
        bind_rows()
      
      # Is this just a summary of the series? Then exit.
      if (any(json_map$WH_STATUS %in% "0")) {
        id_info <- data.frame(gid = x)
        print(paste("Series with game ", x, " does not exist"))
      } else {
        team_ids <- json_map |> 
          select(TEAM_ID, TEAM_ABBREVIATION) |> 
          filter(!is.na(TEAM_ID)) |> 
          distinct() |> 
          column_to_rownames("TEAM_ID")
        
        id_info <- json_map |> 
          filter(table == "GameSummary") |> 
          select_if(~ !any(is.na(.))) |> 
          mutate(date = as.Date(str_sub(GAME_DATE_EST, 1, 10)),
                 home = team_ids[HOME_TEAM_ID,],
                 away = team_ids[VISITOR_TEAM_ID,]) |> 
          select(gid = GAME_ID, gcode = GAMECODE,
                 date, home, away)
        
        score <- json_map |> 
          filter(table == "LineScore") |> 
          select_if(~ !any(is.na(.))) |> 
          mutate(date = as.Date(str_sub(GAME_DATE_EST, 1, 10)),
                 team_abr = TEAM_ABBREVIATION,
                 side = if_else(team_abr == id_info$home, "home", "away"),
                 score = PTS) |> 
          select(gid = GAME_ID, date, side, team_abr, score) |> 
          pivot_wider(id_cols = c("gid", "date"),
                       names_from = "side",
                       names_glue = "{side}_{.value}",
                       values_from = c(team_abr, score)) |> 
          select(gid, date, home = home_team_abr, home_score,
                 away = away_team_abr, away_score)
        
        game_info <- left_join(id_info, score)
      }
      
      return(game_info)
    }
  })
  
  
  yoffs <- bind_rows(yoffs_mapped) |> 
    filter(!is.na(gcode)) |> 
    distinct()
  
  write_csv(yoffs, yoffs_2015_file)
}


# Query for all of the existing schedules for NBA game ids, not sure what to do
#  when a season starts to add on playoff games
end_year <- ifelse(Sys.Date() > paste0(format(Sys.Date(), "%Y"), "-10-01"),
                   as.numeric(format(Sys.Date(), "%Y")),
                   as.numeric(format(Sys.Date(), "%Y")) - 1)
years <- seq(2015, end_year)

ids_map <- map(years, function(x) {
  print(paste(x, "at", Sys.time()))
  
  # Only need to scrape the latest season
  if (file.exists(paste0(id_source, "/game_ids_", x, ".csv")) &
      x != end_year) {
    id_info <- read_csv(paste0(id_source, "/game_ids_", x, ".csv"),
                        col_types = cols(.default = "c")) |> 
      mutate(date = as.Date(date))
    return(id_info)
  } else {
    # If the file didn't exist then pause.
    Sys.sleep(runif(1, 2.5, 5))
    
  }
  # https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/
  #  2021/league/00_full_schedule.json
  x_url <- paste0("https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/",
                  x,
                  "/league/00_full_schedule.json")
  
  res <- request(x_url) |> 
    # req_headers(!!!stats_nba_headers) |> 
    req_perform()
  
  # Exit out if the query is invalid
  if (res$status_code > 400) {
    id_info <- data.frame(gid = NA)
    print(paste("Season ", x, " does not exist"))
  } else {
    id_info <- res |> 
      resp_body_json(simplifyVector = T) |>
      pluck("lscd") |>
      pluck("mscd") |>
      pluck("g") |>
      bind_rows() |> 
      mutate(date = as.Date(gdte)) |> 
      # Unravel the broadcast variables for each game
      unnest(bd, keep_empty = T, names_sep = "_") |>
      unnest(bd_b, keep_empty = T, names_sep = "_") |>
      # Unravel score infos
      unnest(c(v, h), names_sep = "_") |>
      group_by(gid, gcode, date,
               home = h_ta, home_score = h_s,
               away = v_ta, away_score = v_s) |>
      # Extract only the times a game was on national TV, which could be null
      summarise(national_tv = list(bd_b_disp[bd_b_scope == "natl" &
                                               bd_b_type == "tv"])) |>
      # Replace the possible nulls with NA for national TV and unravel
      unnest(national_tv, keep_empty = T) |>
      arrange(date, gid) |> 
      ungroup()
    
    # Save each season game schedule as csv
    write_csv(id_info, paste0(id_source, "/game_ids_", x, ".csv"))
  }
  
  return(id_info)
})

# ---- wayback ------------------------------------------------------------

# Cross for networks that are not national
#  network on left, right says if it is not national
tv_cross <- c(#"ABC" = "",
  "CBC" = "no",
  "CTV" = "no",
  # "ESPN" = "",
  # "ESPN2" = "",
  "HDNET" = "no",
  "NBAC" = "no",
  "NBALP" = "no",
  # "NBATV" = "",
  # "NBC" = "",
  "RSN" = "no",
  "RTV" = "no",
  "SCORE" = "no",
  # "TBS" = "",
  "Telemundo" = "no",
  # "TNT" = "",
  "TSN" = "no",
  "TSN2" = "no",
  "WGN" = "no")

wayback_file <- "0-data/wayback/tv/NBA dot com Wayback TV Schedule - ALL.csv"

if (!file.exists(wayback_file)) {
  dir.create(str_remove(wayback_file, basename(wayback_file)),
             recursive = T)
  download.file(
    paste0("https://raw.githubusercontent.com/atlhawksfanatic",
           "/L2M/master/0-data/wayback/tv/",
           "NBA%20dot%20com%20Wayback%20TV%20Schedule%20-%20ALL.csv"),
    wayback_file
    )
}

wayback <- wayback_file |> 
  read_csv(col_types = cols(.default = "c")) |>
  mutate_all(~str_remove(., "\n")) |> 
  type_convert() |> 
  mutate(date = as.Date(date, "%m/%d/%Y"),
         national_tv = ifelse(is.na(tv_cross[network_1]),
                              network_1,
                              tv_cross[network_1]))

j5 <- bind_rows(pre2015_ids, yoffs) |> 
  left_join(wayback) |> 
  select(gid, gcode, date, home, away, home_score, away_score,
         networks, national_tv) |> 
  replace_na(list(national_tv = "no")) |> 
  mutate_at(vars(home_score, away_score), as.numeric)

# Bring them all together
game_list <- bind_rows(ids_map) |> 
  replace_na(list(national_tv = "no")) |> 
  mutate_at(vars(home_score, away_score), as.numeric) |> 
  bind_rows(j5) |> 
  filter(!is.na(gid), date > "2001-11-12") |> 
  arrange(date, gid)


# Save as csv
write_csv(game_list, paste0(local_dir, "/nba_game_schedule.csv"))
