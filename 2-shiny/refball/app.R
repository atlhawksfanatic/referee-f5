# https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_0042000133.json

library(bslib)
library(shiny)
library(shinythemes)
library(tidyverse)
library(DT)

# ref_calls <- read_csv("2-shiny/refball/datanba_szn_calls_shufinskiy.csv") |> 
#   filter(!is.na(Referee))
ref_calls <- read_csv("datanba_szn_calls_shufinskiy.csv") |> 
  filter(!is.na(Referee))
# ref_teams <- read_csv("2-shiny/refball/datanba_ref_teams_shufinskiy.csv")
ref_teams <- read_csv("datanba_ref_teams_shufinskiy.csv")

ref_totals <- ref_calls |> 
  mutate(season_alpha = str_glue("{min(season)-1}-{max(season)}"),
         season_type = ifelse(season_prefix == "002",
                              "Regular Season",
                              "Playoffs")) |> 
  select(-season_prefix, -season) |> 
  group_by(season_type, Referee, season_alpha) |> 
  summarise_all(sum, na.rm = T)

ref_temp <- ref_calls |>
  mutate(season_alpha = str_glue("{season-1}-{season-2000}"),
         season_type = ifelse(season_prefix == "002",
                              "Regular Season",
                              "Playoffs")) |> 
  bind_rows(ref_totals) |> 
  mutate(mode = "Total")


ref_shiny <- ref_temp |> 
  mutate(across(`Total fouls`:`Too many players technical`,
                ~round(.x/Games, digits = 2)),
         mode = "Per Game") |> 
  bind_rows(ref_temp)


szn_opts <- sort(unique(ref_shiny$season_alpha), decreasing = T)
szn_opts <- c(szn_opts[-which.max(nchar(szn_opts))],
              szn_opts[which.max(nchar(szn_opts))])
szn_type_opts <- unique(ref_shiny$season_type)
mode_opts <- c("Total", "Per Game")

referee_opts <- unique(ref_shiny$Referee, na.rm = T)

# custom ggplot2 theme
theme_owen <- function () { 
  theme_minimal(base_size=9, base_family="Consolas") %+replace% 
    theme(
      panel.grid.minor = element_blank(),
      plot.background = element_rect(fill = 'floralwhite',
                                     color = "floralwhite")
    )
}

ui <- page_navbar(
  title = "Ref Ball",
  theme = shinytheme("simplex"),
  bg = "WHITE",
  # inverse = TRUE,
  nav_panel(title = "Data Tables",
            titlePanel("An Unofficial NBA Ref Ball Database"),
            strong("This table shows the total number (or per game number) of fouls and violations called by a given referee in a specific season"),
            h5(a("Original version", href="https://llewellynjean.shinyapps.io/NBARefDatabase/"),
               " Created By ",
               a("Owen Phillips", href="https://twitter.com/owenlhjphillips"),
               " With NBA Play-by-Play Data"),
            h5(a("Current iteration", href="https://github.com/atlhawksfanatic/referee-f5"),
               " created by ",
               a("robert", href="https://twitter.com/atlhawksfanatic")),
            fluidRow(
              column(width = 1, selectInput("season_alpha", "Season:", szn_opts)),
              column(width = 1, selectInput("season_type", "Season Type:", szn_type_opts)),
              column(width = 1, selectInput("mode", "Mode:", mode_opts)),
              column(width = 2, downloadButton("download_data", "Download Data"))
            ),
            dataTableOutput("dynamic")
            ),
  
  nav_panel(title = "Referee Comparison Tool",
            titlePanel("Referee Comparison Tool"),
            strong("This chart (and the accompanying table below it) shows how a given referee compares to their peers in terms of fouls called per game during the Regular Season only"),
            h5(a("Original version", href="https://llewellynjean.shinyapps.io/NBARefDatabase/"),
               " Created By ",
               a("Owen Phillips", href="https://twitter.com/owenlhjphillips"),
               " With NBA Play-by-Play Data"),
            h5(a("Current iteration", href="https://github.com/atlhawksfanatic/referee-f5"),
               " created by ",
               a("robert", href="https://twitter.com/atlhawksfanatic")),
            p("Second page content.")),
  
  nav_panel(title = "Referee & Team Combinations",
            titlePanel("Referee & Team Combinations"),
            strong("This table shows the number of fouls per game a specific referee called against a given team between 2016 - 2020 (Regular Season + Playoffs)"),
            h5(a("Original version", href="https://llewellynjean.shinyapps.io/NBARefDatabase/"),
               " Created By ",
               a("Owen Phillips", href="https://twitter.com/owenlhjphillips"),
               " With NBA Play-by-Play Data"),
            h5(a("Current iteration", href="https://github.com/atlhawksfanatic/referee-f5"),
               " created by ",
               a("robert", href="https://twitter.com/atlhawksfanatic")),
            fluidRow(
              column(width = 5, sliderInput("slider", "Minimum Games:",
                                            min = 1, max = max(ref_teams$Games),
                                            value = 10))
            ),
            dataTableOutput("ref_teams")
            ),
  nav_spacer(),
  nav_menu(
    title = "Links",
    align = "right",
    nav_item(tags$a("Posit", href = "https://posit.co")),
    nav_item(tags$a("Shiny", href = "https://shiny.posit.co"))
  )
)

# # Define UI for application that draws a histogram
# ui <- fluidPage(
#   # Application title
#   titlePanel("An Unofficial NBA Ref Ball Database"),
#   
#   # Sidebar with a slider input for number of bins 
#   sidebarLayout(
#     sidebarPanel(
#       sliderInput("bins",
#                   "Number of bins:",
#                   min = 1,
#                   max = 50,
#                   value = 30)
#       ),
#     
#     # Show a plot of the generated distribution
#     mainPanel(
#       plotOutput("distPlot")
#       )
#     )
#   )


# Define server logic required to draw a histogram
server <- function(input, output) {
  output$dynamic <- renderDataTable({
    ref_shiny |> 
      filter(season_alpha == input$season_alpha,
             season_type == input$season_type,
             mode == input$mode) |> 
      select(Referee:`Too many players technical`)
  }, options = list(pageLength = 100))
  
  output$ref_teams <- renderDataTable({
    ref_teams |> 
      filter(Games >= input$slider) |> 
      mutate(across(c(-Referee, -Team, -Games),
                    ~round(.x, digits = 2)))
  }, options = list(pageLength = 100))

}

# Run the application 
shinyApp(ui = ui, server = server)
