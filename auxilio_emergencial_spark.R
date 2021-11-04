

#########################################################################################################
############################ Auxilio Emergencial com R + Spark ##########################################
#########################################################################################################
#####   ESCRITO POR:      Cleiton Rocha - www.linkedin.com/in/cleitonoerocha/                          ##  
#####   EMAIL:  cleiton.rocha@fiocruz.br // cleitonotavio058@gmail.com                                 ##
#####   LICENÇA:          GPLv3                                                                        ##
#####   Data: 03/11/2021                                                                               ##
#########################################################################################################
#########################################################################################################


#install.packages(c("sparklyr", "arrow", "tidyverse", "geobr", "readxl", "Cairo))

library(tidyverse)
library(sparklyr)
library(arrow)
library(readxl)
library(geobr)
library(Cairo)


options(scipen = 999)


############################################################################
########## Projeção Populacional da UFRN -----------------------------------
############################################################################


proj_pop <- read_excel("ProjMunic-2010_2030.xlsx")


# Total da população maior ou igual a 15 anos --------
proj_pop_total <- proj_pop %>%
  filter(Ano == 2020) %>%
  group_by(Armenor) %>%
  summarise(total_pop = sum(Total) - sum(`0 a 5`, `5 a 10`, `10 a 15`)) 


############################################################################
############### Aux. Emerg. ------------------------------------------------
############################################################################


# Criando uma conexao com o Spark ---------------------------
spark_conn <- spark_connect(master = "local")


# Verificando o status da conexão ------------------------
spark_connection_is_open(spark_conn)


# Carregando os microdados do aux emergencial no Spark -------------------
aux_emerg_202012 <- spark_read_csv(
  sc = spark_conn,
  name = "aux_emerg_202012",
  path = "202012_AuxilioEmergencial.csv",
  charset = "Latin1",
  delimiter = ";",
  memory = FALSE
) 


# Total de beneficiários em cada município ----------------------------------
aux_emerg_202012_mun <- aux_emerg_202012 %>%
                        filter(!is.na(CADIGO_MUNICAPIO_IBGE)) %>% 
                        group_by(CADIGO_MUNICAPIO_IBGE) %>%
                        summarise(total_beneficiarios = n()) %>% 
                        collect()


######### Análise descritiva dos dados ------------------------------------------

# pop brasil 2020
pop_brasil_2020 <- proj_pop %>% filter(Ano == 2020) %>% summarise(total_pop_brasil = sum(Total)) %>% as.numeric()

# pop brasil 2020 mais de 15 anos
pop_brasil_2020_mais_15 <- sum(proj_pop_total$total_pop) %>% as.numeric()


###### Total de beneficiários em dezembro ----------------------------
paste0(format(sum(aux_emerg_202012_mun$total_beneficiarios), big.mark = ".", decimal.mark = ","))


###### Proporção de beneficiários na população brasileira -----------------------
paste0(format(round((sum(aux_emerg_202012_mun$total_beneficiarios)/pop_brasil_2020)*100,1),
              big.mark = ".",
              decimal.mark = ","),
       "%")


#### Proporção de beneficiários na população brasileira maior ou igual a 15 anos -----------------------
paste0(format(round((sum(aux_emerg_202012_mun$total_beneficiarios)/pop_brasil_2020_mais_15)*100,1),
              big.mark = ".",
              decimal.mark = ","),
       "%")



#### Número da parcela do auxílio ------------------------------------------
numero_parcela <- aux_emerg_202012 %>%
  filter(!is.na(CADIGO_MUNICAPIO_IBGE)) %>% 
  group_by(PARCELA) %>%
  summarise(total_beneficiarios = n()) %>% 
  collect()


numero_parcela <- numero_parcela %>% mutate(percentual = paste0(round((total_beneficiarios/sum(total_beneficiarios))*100,2),"%"))


# Quantis da população beneficiada ------------------------
quantile(aux_emerg_202012_mun$total_beneficiarios)


#######################################################################################
######## Gerando mapa -----------------------------------------------------------------
#######################################################################################


# Shapefile dos municípios --------------------------
mun <- read_municipality(code_muni="all", year=2020)


# Shapefile das UF' ---------------------------------
estado <- read_state(code_state = "all", year = 2020)


aux_emerg_202012_mun_pop <- inner_join(aux_emerg_202012_mun, proj_pop_total, by = c("CADIGO_MUNICAPIO_IBGE" = "Armenor"))

aux_emerg_202012_mun_pop <- aux_emerg_202012_mun_pop %>% mutate(razao_aux_pop = round(((total_beneficiarios/total_pop)*100),1))


# Criando variável categórica --------------------------------------
aux_emerg_202012_mun_pop$total_benef_categorica <- cut(aux_emerg_202012_mun_pop$razao_aux_pop, c(0,20,40,60,1000000000), 
                                                   labels=c("0,0% - 20,0%",
                                                            "20,1% - 40,0%",
                                                            "40,1% - 60,0%",
                                                            ">= 60,1%"),
                                                   rigth=T, exclude=NULL, include.lowest = TRUE)



# Unindo dataframe com shapefile ------------------------------------
aux_emerg_join_map <- left_join(mun, aux_emerg_202012_mun_pop, by=c("code_muni" = "CADIGO_MUNICAPIO_IBGE"))



# Mapa --------------------------------------------------------
all_mun_graph <- ggplot() +
  geom_sf(data = aux_emerg_join_map, aes(fill = total_benef_categorica), color = NA, size = .15)+
  geom_sf(data = estado,size=.15, fill = NA, color = "black", show.legend = FALSE) +
  labs(color="",
       title = "Percentual de beneficiários do Auxílio Emergencial, por município do Brasil. 2020.",
       caption = "Fonte: Microdados do Auxílio Emergencial (Dezembro de 2020). Portal da Transparência\nElaboração: Cleiton Rocha (@CleitonOERocha/cleitonotavio058@gmail.com)", 
       fill = "% de beneficiários na\npopulação do município: ") +
  theme_minimal() +
  theme(plot.title = element_text(face="bold", colour = "black", size = 15, hjust=0.5),
        plot.caption = element_text(size = 10),
        legend.position = "bottom",
        legend.text = element_text(size = 8, hjust=0.5),
        legend.title = element_text(size = 10, hjust=0.5)) +
  scale_fill_brewer(palette = "YlOrRd") 


ggsave(plot = all_mun_graph, "aux_emerg_plot.png",
       width = 12, height = 6,  units = "in", type = "cairo-png")


  
############################################################################################
############ Boxplot -----------------------------------------------------------------------
############################################################################################


# Obtendo o código dos municípios -------------------------
aux_emerg_202012_mun_pop$UF <- substr(aux_emerg_202012_mun_pop$CADIGO_MUNICAPIO_IBGE, 1, 2)


# Convertendo coluna em númerica -------------------------------
aux_emerg_202012_mun_pop$UF <- as.numeric(aux_emerg_202012_mun_pop$UF)


# Criando colunas de UF e Região ---------------------------------------
aux_emerg_202012_mun_pop <- aux_emerg_202012_mun_pop %>% mutate(Estado = case_when(UF == 11 ~ "Rondônia" , 
                                                                                   UF == 12 ~ "Acre" , 
                                                                                   UF == 13 ~ "Amazonas" , 
                                                                                   UF == 14 ~ "Roraima" , 
                                                                                   UF == 15 ~ "Pará" , 
                                                                                   UF == 16 ~ "Amapá" , 
                                                                                   UF == 17 ~ "Tocantins" , 
                                                                                   UF == 21 ~ "Maranhão" , 
                                                                                   UF == 22 ~ "Piauí" , 
                                                                                   UF == 23 ~ "Ceará" , 
                                                                                   UF == 24 ~ "Rio Grande do Norte" , 
                                                                                   UF == 25 ~ "Paraíba" , 
                                                                                   UF == 26 ~ "Pernambuco" , 
                                                                                   UF == 27 ~ "Alagoas" , 
                                                                                   UF == 28 ~ "Sergipe" , 
                                                                                   UF == 29 ~ "Bahia" , 
                                                                                   UF == 31 ~ "Minas Gerais" , 
                                                                                   UF == 32 ~ "Espírito Santo" , 
                                                                                   UF == 33 ~ "Rio de Janeiro" , 
                                                                                   UF == 35 ~ "São Paulo" , 
                                                                                   UF == 41 ~ "Paraná" , 
                                                                                   UF == 42 ~ "Santa Catarina" , 
                                                                                   UF == 43 ~ "Rio Grande do Sul" , 
                                                                                   UF == 50 ~ "Mato Grosso do Sul" , 
                                                                                   UF == 51 ~ "Mato Grosso" , 
                                                                                   UF == 52 ~ "Goiás" , 
                                                                                   UF == 53 ~ "Distrito Federal"),
                                                                Regiao = case_when(UF == 11 ~ "Norte",
                                                                                   UF == 12 ~ "Norte" , 
                                                                                   UF == 13 ~ "Norte" , 
                                                                                   UF == 14 ~ "Norte" , 
                                                                                   UF == 15 ~ "Norte" , 
                                                                                   UF == 16 ~ "Norte" , 
                                                                                   UF == 17 ~ "Norte" , 
                                                                                   UF == 21 ~ "Nordeste" , 
                                                                                   UF == 22 ~ "Nordeste" , 
                                                                                   UF == 23 ~ "Nordeste" , 
                                                                                   UF == 24 ~ "Nordeste" , 
                                                                                   UF == 25 ~ "Nordeste" , 
                                                                                   UF == 26 ~ "Nordeste" , 
                                                                                   UF == 27 ~ "Nordeste" , 
                                                                                   UF == 28 ~ "Nordeste" , 
                                                                                   UF == 29 ~ "Nordeste" ,
                                                                                   UF == 31 ~ "Sudeste" , 
                                                                                   UF == 32 ~ "Sudeste" , 
                                                                                   UF == 33 ~ "Sudeste" , 
                                                                                   UF == 35 ~ "Sudeste" ,
                                                                                   UF == 41 ~ "Sul" , 
                                                                                   UF == 42 ~ "Sul" , 
                                                                                   UF == 43 ~ "Sul" ,
                                                                                   UF == 50 ~ "Centro-Oeste" , 
                                                                                   UF == 51 ~ "Centro-Oeste" , 
                                                                                   UF == 52 ~ "Centro-Oeste" , 
                                                                                   UF == 53 ~ "Centro-Oeste"))


aux_emerg_202012_mun_pop_boxplot <- aux_emerg_202012_mun_pop %>% filter(razao_aux_pop<=100)


#### Plot Região ---------------------------------------
boxplot_reg_aux <- ggplot(aux_emerg_202012_mun_pop_boxplot, aes(x=Regiao, y=razao_aux_pop, fill=Regiao)) +
  geom_boxplot() +
  coord_flip() +
  geom_jitter(aes(color = Regiao),
                width=0.15, alpha = 0.6)+ 
  theme_minimal() +
  labs(y = "% de beneficiários na população dos municípios",
       title = "% de beneficiários do Auxílio Emergencial na população de cada município, por região do Brasil.",
       caption = "Fonte: Microdados do Auxílio Emergencial (Dezembro de 2020). Portal da Transparência\nElaboração: Cleiton Rocha (@CleitonOERocha/cleitonotavio058@gmail.com)", 
       x = "") +
  theme_minimal() +
  theme(plot.title = element_text(face="bold", colour = "black", size = 15, hjust=0.5),
        plot.caption = element_text(size = 10),
        legend.position = "none",
        axis.text.x = element_text(color = "black", face = "bold", size = 9),
        axis.text.y = element_text(color = "black",  face = "bold", size = 9),
        legend.text = element_text(size = 8, hjust=0.5),
        legend.title = element_text(size = 10, hjust=0.5),
        legend.background = element_rect(fill="ghostwhite",
                                         size=0.7,
                                         linetype="blank")) +
  scale_y_continuous(breaks = seq(0,100,5), limits = c(0,100)) +
  scale_fill_manual(values = c("#003f5c", "#58508d", "#bc5090", "#ff6361", "#ffa600")) +
  scale_colour_manual(values = c("#003f5c", "#58508d", "#bc5090", "#ff6361", "#ffa600"))



  # salvando
  ggsave(plot = boxplot_reg_aux,
         "boxplot_reg_aux.png",
         width = 12, height = 6, units = "in",type = "cairo-png")
  
  

### Plot UF -----------------------------------------------
  boxplot_uf_aux <- ggplot(aux_emerg_202012_mun_pop, aes(x = razao_aux_pop, y = reorder(Estado, razao_aux_pop, FUN = median))) +
    geom_boxplot(fill = "#e74c3c") +
    theme_minimal() +
    labs(x = "% de beneficiários na população dos municípios",
         fill = "",
         title = "% beneficiários do Auxílio Emergencial na população de cada município, por Unidade da Federação. Brasil.",
         caption = "Fonte: Microdados do Auxílio Emergencial (Dezembro de 2020). Portal da Transparência\nElaboração: Cleiton Rocha (@CleitonOERocha/cleitonotavio058@gmail.com)", 
         y = "") +
    theme_minimal() +
    theme(plot.title = element_text(face="bold", colour = "black", size = 13, hjust=0.5),
          plot.caption = element_text(size = 10),
          legend.position = "none",
          axis.text.x = element_text(color = "black", face = "bold", size = 9),
          axis.text.y = element_text(color = "black",  face = "bold", size = 9),
          legend.text = element_text(size = 8, hjust=0.5),
          legend.title = element_text(size = 10, hjust=0.5),
          legend.background = element_rect(fill="ghostwhite",
                                           size=0.7,
                                           linetype="blank")) +
    scale_x_continuous(breaks = seq(0,100,5), limits = c(0,100))
  
  
  # salvando
  ggsave(plot = boxplot_uf_aux,
         "boxplot_uf_aux.png",
         width = 13, height = 6.5, units = "in",type = "cairo-png")
