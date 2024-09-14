library(tidyverse)
library("scales")

parallel <- read.csv("ablative_scaling_data.csv")

ggplot() +
  
  geom_point(data = parallel, mapping = aes(x = threads, y = ingest_rate, color = system, shape = system), size = 2) +
  #guides(color = "none") +
  geom_line(data = parallel, mapping = aes(x = threads, y = ingest_rate, color = system), size = 1) +
  #scale_x_continuous(labels=comma, trans="log10") +
  labs(y=expression(paste("Ingestion rate (", ~10^6,"edges/s)")), x ="Number of threads") +
  #guides(color=guide_legend(nrow=2,byrow=TRUE), linetype="none") +
  
  theme(
    legend.position = "bottom",
    axis.title.y = element_text(hjust=0.9)
  ) +
  theme(panel.grid.minor.y = element_blank()) +
  #annotate("text", x = 1000, y = 100, size = 20, color = "red", label="FAKE DATA") +
  scale_y_continuous(labels=comma)  +
  scale_color_hue(labels = c("All features", "no PHT", "no PHT or CameoSketch" )) +
  scale_shape_manual(values = c(15,16,17), labels = c("All features", "no PHT", "no PHT or CameoSketch"))
ggsave(filename = "ablative.png", width = 6, height = 3, unit = "in")
