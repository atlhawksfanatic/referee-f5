# Get pbp data
# https://github.com/shufinskiy/nba_data?tab=readme-ov-file

# To Do: identify the current season/playoff and re-download data if stale

# ---- start --------------------------------------------------------------

library(tidyverse)

# Create directory and .gitignore
local_dir <- "0-data/shufinskiy"
data_source <- str_glue("{local_dir}/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source, recursive = T)

# ignore raw folders
filecon <- file(paste0(local_dir, "/.gitignore"))
writeLines("raw", filecon)
close(filecon)

# ---- download -----------------------------------------------------------

shfky_url <- paste0("https://raw.githubusercontent.com/shufinskiy/",
                    "nba_data/main/list_data.txt")

shfky_files <- read_lines(shfky_url)
tar_files   <- as.data.frame(str_split(shfky_files, "=", simplify = T))

tar_files |> 
  mutate(szn = str_extract(V1, "[0-9]+"),
         temp = str_remove(V1, str_glue("_{szn}")),
         topic = str_remove(temp, "_po"),
         szn_type = ifelse(grepl("_po", temp),
                           "playoffs",
                           "regular_season")) |> 
  filter(grepl("(nbastats)|(pbpstats)|(datanba)", topic)) |> 
  select(-temp) |> 
  pmap(function(V1, V2, szn, szn_type, topic) {
    temp_folder <- str_glue("{data_source}/{topic}")
    if (!file.exists(temp_folder)) dir.create(temp_folder, recursive = T)
    temp_file <- str_glue("{temp_folder}/{szn}_{szn_type}.tar.xz")
    
    if (!file.exists(temp_file)) {
      download.file(V2, temp_file)
      Sys.sleep(2)
    }
    untar(temp_file, exdir = temp_folder)
    
    print(str_glue("{temp_file} downloading at {Sys.time()}"))
  })
