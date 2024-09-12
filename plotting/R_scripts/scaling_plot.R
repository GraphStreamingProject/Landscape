library(tidyverse)
library("scales")

parallel <- read.csv("scaling_data.csv")
the_point <- tibble(
  threads = 2560, 
  insertion_rate = 223,
)

ggplot() +
  geom_line(data = parallel, mapping = aes(x = threads, y = insertion_rate), size = 1) +
  geom_point(data = parallel, mapping = aes(x = threads, y = insertion_rate), size = 2) +
  #geom_point(data = the_point, aes(x = threads, y=insertion_rate), color = "red", size = 4) +
  #scale_x_continuous(labels=comma, trans="log10") +
  labs(y=expression(paste("Ingestion rate (", ~10^6,"edges/s)")), x ="Number of threads") +
  guides(color=guide_legend(nrow=2,byrow=TRUE), linetype="none") +
  theme(panel.grid.minor.y = element_blank()) +
  #annotate("text", x = 1000, y = 100, size = 20, color = "red", label="FAKE DATA") +
  scale_y_continuous(labels=comma) 
ggsave(filename = "scaling.png", width = 6, height = 3, unit = "in")
