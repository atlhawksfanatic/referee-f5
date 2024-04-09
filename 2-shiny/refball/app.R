
# ---- start --------------------------------------------------------------

library(bslib)
library(shiny)
library(shinythemes)
library(tidyverse)
library(DT)

# ---- functions ----------------------------------------------------------

# custom ggplot2 theme
theme_owen <- function (...) { 
  theme_minimal(base_size = 20,
                base_family = "Consolas") %+replace% 
    theme(...,
          panel.grid.minor = element_blank(),
          plot.background = element_rect(fill = 'floralwhite',
                                         color = "floralwhite")
    )
}

gg_arrows <- data.frame(
  x = c(3, -3), xend = c(4, -4),
  y = 5.25, yend = 5.25,
  caption = c("Calls more than average", "Calls less than average")
)


gg_referee <- function(dataset, x_ref, x_szn) {
  dataset |> 
    filter(season_alpha == x_szn) |>
    mutate(highlight = Referee == x_ref,
           Games = ifelse(highlight, 200, Games),
           labelz = ifelse(highlight, var, NA)) |> 
    ggplot(aes(z, var)) +
    geom_point(aes(color = highlight, alpha = Games),
               position = position_jitter(w = 0, h = 0.05), size = 3) +
    geom_label(aes(label = labelz),
               nudge_y = -0.25,
               size = 4,
               color = "black") +
    geom_vline(xintercept = 0, linetype = "dotted") +
    geom_segment(data = gg_arrows, aes(x, y, xend = xend, yend = yend),
                 arrow = arrow(length = unit(0.25, "cm"))) +
    geom_text(data = gg_arrows, aes(x = (x + xend) / 2,
                                    y = y + 0.25,
                                    label = caption)) +
    scale_x_continuous(limits = c(-4.5, 4.5),
                       breaks = seq(-4.5, 4.5, by = 0.5)) +
    scale_color_manual(values = c("black", "red")) +
    # scale_alpha_continuous(range = c(0.1, 0.5)) +
    labs(title = str_glue("{x_ref} ({x_szn})"),
         subtitle = str_glue("Foul Calls Per Game In The Regular Season ",
                             "Relative To A League Average Referee\n",
                             "Among referees that officiated 10 or more games"),
         caption = "Credit: Owen Phillips\n recreated by @atlhawksfanatic",
         y = "", x = "Normalized Per Game Value (Z-Score)") +
    theme_owen(axis.text.y=element_blank(),
               legend.position = "none",
               plot.title = element_text(hjust = 0.5, color = "red"),
               plot.subtitle = element_text(hjust = 0.5))
  
  
}


# ---- data ---------------------------------------------------------------

# ref_calls <- read_csv("2-shiny/refball/datanba_szn_calls_shufinskiy.csv") |>
#   filter(!is.na(Referee))
# ref_teams <- read_csv("2-shiny/refball/datanba_ref_teams_shufinskiy.csv")
# ref_gg <- read_csv("2-shiny/refball/datanba_ggplot_shufinskiy.csv")

ref_calls <- read_csv("datanba_szn_calls_shufinskiy.csv") |> 
  filter(!is.na(Referee))
ref_teams <- read_csv("datanba_ref_teams_shufinskiy.csv")
ref_gg <- read_csv("datanba_ggplot_shufinskiy.csv")


# ---- some-opts ----------------------------------------------------------

szn_opts <- sort(unique(ref_calls$season_alpha), decreasing = T)
szn_opts <- c(szn_opts[-which.max(nchar(szn_opts))],
              szn_opts[which.max(nchar(szn_opts))])
szn_type_opts <- unique(ref_calls$season_type)
mode_opts <- c("Total", "Per Game")

referee_opts <- unique(na.omit(ref_gg$Referee))

max_date <- max(ref_calls$max_date)

# ---- user-interface -----------------------------------------------------

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
            h5(str_glue("Play-by-play data through {max_date}")),
            fluidRow(
              column(width = 1, selectInput("season_alpha", "Season:", szn_opts)),
              column(width = 1, selectInput("season_type", "Season Type:", szn_type_opts)),
              column(width = 1, selectInput("mode", "Mode:", mode_opts)),
              column(width = 3, downloadButton("download_data", "Download Data"))
            ),
            fluidRow(
            column(6, dataTableOutput("dynamic"))
            )
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
            h5(str_glue("Play-by-play data through {max_date}")),
            fluidRow(
              column(width = 2, selectInput("season_alpha", "Season:", szn_opts)),
              column(width = 2, selectInput("referee", "Referee:", referee_opts))
            ),
            fluidRow(
              plotOutput("gg_one", height = "700px")
            ),
            dataTableOutput("gg_data")),
  
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
            h5(str_glue("Play-by-play data through {max_date}")),
            fluidRow(
              column(width = 5, sliderInput("slider", "Minimum Games:",
                                            min = 1, max = max(ref_teams$Games),
                                            value = 10))
            ),
            fluidRow(
              column(7, dataTableOutput("ref_teams"))
            )
            ),
  nav_spacer(),
  nav_menu(
    title = "Links",
    align = "right",
    nav_item(tags$a("Posit", href = "https://posit.co")),
    nav_item(tags$a("Shiny", href = "https://shiny.posit.co"))
  )
)

# ---- server -------------------------------------------------------------

# Define server logic required to draw a histogram
server <- function(input, output) {
  output$dynamic <- renderDataTable({
    ref_calls |> 
      filter(season_alpha == input$season_alpha,
             season_type == input$season_type,
             mode == input$mode) |> 
      select(Referee:`Too many players technical`)
  }, options = list(pageLength = 100,
                    info = FALSE,
                    scrollX = TRUE))
  
  ref_gg_server <- reactive({
    ref_gg |> 
      filter(season_alpha == input$season_alpha)
  })
  
  observeEvent(input$season_alpha, {
    updateSelectInput(inputId = "referee",
                choices = referee_opts <-
                  unique(na.omit(ref_gg_server()$Referee)))
  })
  
  output$gg_one <- renderPlot({
    gg_referee(ref_gg_server(), input$referee, input$season_alpha)
  })
  
  output$gg_data <- renderDT({
    ref_gg |> 
      filter(season_alpha == input$season_alpha,
             Referee == input$referee) |> 
      mutate(across(c(val, league_avg, z), ~round(., digits = 2))) |> 
      select(Referee, Category = var, "Per Game" = val,
             "League Average" = league_avg,
             "Z-Score" = z)
  }, options = list(paging = FALSE,
                    searching = FALSE,
                    info = FALSE),
  rownames = FALSE)
  
  output$ref_teams <- renderDataTable({
    ref_teams |> 
      filter(Games >= input$slider) |> 
      mutate(across(c(-Referee, -Team, -Games),
                    ~round(.x, digits = 2)))
  }, options = list(pageLength = 100,
                    info = FALSE))

}

# ---- run ----------------------------------------------------------------


# Run the application 
shinyApp(ui = ui, server = server)
