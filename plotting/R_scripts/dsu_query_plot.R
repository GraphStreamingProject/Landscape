library(tidyverse)
library("scales")

queries <- read.csv("dsu_query.csv")
trimmed_queries <-  filter(queries, burst != 6)
trimmed_queries <- filter(trimmed_queries, system != "gz")

ggplot() + 
  geom_jitter(data = trimmed_queries, mapping = aes(x = burst, y = flush_latency+boruvka_latency, color=query_type), width=0.1, size = 3, shape="diamond") +
  
  scale_x_continuous(labels=comma) +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    #panel.grid.major.y = element_blank(),
    #panel.grid.minor.y = element_blank()
    legend.position = "bottom"
  ) +
  scale_color_discrete(name = "Query Type", labels = c("Global","Batched Reachability")) +
  #annotate("text", x = 2.3, y = 1, size = 20, color = "red", label="FAKE DATA") +
  labs(y=expression(paste("Query Latency (s)")), x ="burst") +
  #guides(color=guide_legend(nrow=2,byrow=TRUE)) +
  scale_y_continuous(trans="log10") 
ggsave(filename = "dsu_query.png", width = 6, height = 3, unit = "in")
