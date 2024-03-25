
library(bslib)
library(shiny)
library(shinythemes)

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
            p("First page content.")),
  
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
            p("Third page content.")),
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
  output$distPlot <- renderPlot({
    # generate bins based on input$bins from ui.R
    x    <- faithful[, 2]
    bins <- seq(min(x), max(x), length.out = input$bins + 1)
    
    # draw the histogram with the specified number of bins
    hist(x, breaks = bins, col = 'darkgray', border = 'white',
         xlab = 'Waiting time to next eruption (in mins)',
         main = 'Histogram of waiting times')
  })
}

# Run the application 
shinyApp(ui = ui, server = server)
