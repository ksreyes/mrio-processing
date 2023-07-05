# GVC PARTICIPATION - CHECKS
# July 2023

rm(list = ls())
library(tidyverse)
library(here)


# Load data ----

df <- here("data", "final", "gvcp.csv") %>%
  read_csv() %>%
  filter(agg == 5) %>%
  mutate(i = factor(i))

# df <- here("data", "final", "gvcp.csv") %>%
#   read_csv() %>%
#   filter(agg == 5 & t <= 2019) %>%
#   mutate(i = factor(i))
# 
# dfchecks <- here("data", "final", "gvcp_jun2023.csv") %>%
#   read_csv() %>%
#   filter(agg == 5) %>%
#   mutate(i = factor(i))
# 
# df <- df %>%
#   bind_rows(dfchecks)

countries <- df %>% pull(s) %>% unique()
sectors <- df %>% pull(i) %>% unique()


# GVCP_trade ----

for (country in countries) {
  
  df_s <- df %>% 
    filter(s == country)
  
  max = df_s %>% pull(GVCP_trade) %>% max()
  
  plot <- ggplot(df_s, aes(x = t, y = GVCP_trade, color = i, group = i)) +
    geom_line(linewidth = 1) +
    scale_color_manual(values = c("#007DB7", "#C8DA2B", "#F57F29", "#63CCEC", "#FDB415")) +
    scale_y_continuous(
      limits = c(0, max),
      labels = function(x)
        paste0(100 * x, "%")
    ) +
    theme(
      axis.title = element_blank(),
      axis.line = element_line(linewidth = .25, color = "black"),
      axis.ticks = element_line(linewidth = .25, color = "black"),
      axis.text.x = element_text(
        size = 9,
        color = "black",
        margin = margin(5, 0, 0, 0)
      ),
      axis.text.y = element_text(size = 9, color = "black"),
      legend.title = element_blank(),
      legend.position = "top",
      legend.background = element_blank(),
      legend.spacing = unit(0, "lines"),
      legend.key = element_blank(),
      legend.key.size = unit(.75, "lines"),
      legend.text = element_text(size = 9),
      panel.background = element_blank(),
      panel.border = element_blank(),
      panel.grid = element_blank()
    )
  
  ggsave(
    # paste0("checks/gvcptrade_dec2022/gvcptrade-", country, "_dec2022.pdf"),
    paste0("checks/gvcptrade_jun2023/gvcptrade-", country, "_jun2023.pdf"),
    plot,
    device = cairo_pdf,
    width = 16,
    height = 10,
    unit = "cm"
  )
}


# GVCP_prod ----

for (country in countries) {
  
  df_s <- df %>% 
    filter(s == country)
  
  max = df_s %>% pull(GVCP_prod) %>% max()
  
  plot <- ggplot(df_s, aes(x = t, y = GVCP_prod, color = i, group = i)) +
    geom_line(linewidth = 1) +
    scale_color_manual(values = c("#007DB7", "#C8DA2B", "#F57F29", "#63CCEC", "#FDB415")) +
    scale_y_continuous(
      limits = c(0, max),
      labels = function(x)
        paste0(100 * x, "%")
    ) +
    theme(
      axis.title = element_blank(),
      axis.line = element_line(linewidth = .25, color = "black"),
      axis.ticks = element_line(linewidth = .25, color = "black"),
      axis.text.x = element_text(
        size = 9,
        color = "black",
        margin = margin(5, 0, 0, 0)
      ),
      axis.text.y = element_text(size = 9, color = "black"),
      legend.title = element_blank(),
      legend.position = "top",
      legend.background = element_blank(),
      legend.spacing = unit(0, "lines"),
      legend.key = element_blank(),
      legend.key.size = unit(.75, "lines"),
      legend.text = element_text(size = 9),
      panel.background = element_blank(),
      panel.border = element_blank(),
      panel.grid = element_blank()
    )
  
  ggsave(
    paste0("checks/gvcpprod_dec2022/gvcpprod-", country, "_dec2022.pdf"),
    # paste0("checks/gvcpprod_jun2023/gvcpprod-", country, "_jun2023.pdf"),
    plot,
    device = cairo_pdf,
    width = 16,
    height = 10,
    unit = "cm"
  )
}