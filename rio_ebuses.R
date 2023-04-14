library(dplyr)
library(data.table)
library(gtfstools)


gtfs_rio <- read_gtfs("../../../Downloads/gtfs_rio-de-janeiro.zip")


unique(gtfs_rio$routes$route_id) %>% length()

# get regular day
gtfs_rio$calendar
gtfs_rio_week <- gtfstools::filter_by_service_id(gtfs_rio, service_id = "U")
gtfs_rio_saturday <- gtfstools::filter_by_service_id(gtfs_rio, service_id = "S")
gtfs_rio_sunday <- gtfstools::filter_by_service_id(gtfs_rio, service_id = "D")

# trip length
trips_length <- get_trip_length(gtfs_rio, file = "shapes")

# get the number of trips by each one of the trip_id
trips_times <- gtfs_rio$frequencies %>%
  # get the weekday
  left_join(gtfs_rio$trips %>% select(trip_id, service_id)) %>%
  filter(service_id %in% c("U", "S", "D")) %>%
  mutate(start_time = as.ITime(start_time), end_time = as.ITime(end_time)) %>%
  filter(!is.na(start_time)) %>%
  mutate(end_time = ifelse(is.na(end_time), as.ITime("23:59:59"), end_time)) %>%
  mutate(start_time = as.ITime(start_time), end_time = as.ITime(end_time)) %>%
  # calculate duration
  mutate(duration = as.numeric(end_time - start_time)) %>%
  # calculate the number of trips
  mutate(trips_n = floor(duration / headway_secs)) %>%
  select(trip_id, trips_n, service_id)


# bring to the total km
trips_length_final <- trips_length %>%
  left_join(trips_times, by = "trip_id") %>%
  mutate(length_total = length * trips_n)
# calculate yearly
trips_length_final_year <- trips_length_final %>%
  mutate(times = ifelse(service_id == "U", 251, 53)) %>%
  group_by(service_id) %>%
  summarise(vkt = sum(length_total * times))

sum(trips_length_final_year$vkt, na.rm = TRUE)






# vai -----------------------------------------------------------------------------------------


frota <- fread("../../../Downloads/Cadastro de Veiculos.txt")

# project id
# basedosdados-380121

library(basedosdados)

set_billing_id("basedosdados-380121")

query <- "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`"
dir <- tempdir()
data <- download(query, "<PATH>")



# api -----------------------------------------------------------------------------------------

library(httr)


oi <- GET("https://jeap.rio.rj.gov.br/dadosAbertosAPI/v2/transporte/veiculos/onibus2")

oi1 <- content(oi)

# Parsing data in JSON
get_movie_json <- fromJSON(get_movie_text,
                           flatten = TRUE)
get_movie_json

# Converting into dataframe
get_movie_dataframe <- as.data.frame(get_movie_json)


# ---------------------------------------------------------------------------------------------

# Para carregar o dado direto no R
tb <- read_sql( "SELECT * FROM `datario.transporte_rodoviario_municipal.viagem_onibus` LIMIT 1000" )
tb <- read_sql( "SELECT * FROM `datario.transporte_rodoviario_municipal.viagem_onibus` WHERE datetime_partida BETWEEN '2023-01-06 00:00:00' AND '2023-01-06 23:59:59'" )
tb <- read_sql( "SELECT * FROM `datario.transporte_rodoviario_municipal.viagem_onibus` WHERE datetime_partida BETWEEN '2022-07-01 00:00:00' AND '2022-08-31 23:59:59'" )
# save
readr::write_rds(tb, "data/trips_rio_202207-202208.rds")

tb <- readr::read_rds("data/trips_rio_202207-202208.rds")


tb1 <- tb %>%
  mutate(dia = as.Date(datetime_partida))

table(tb1$dia)

sum(tb1_dia$distancia_planejada)  

# juntar com a base dos veiculos

veiculos <- fread("../../../Downloads/Cadastro de Veiculos.txt") %>%
  select(id_veiculo = V2, type_vehicle = V17, euro = V12, fuel = V18) %>%
  # format the vehicle type
  mutate(vehicle_type = stringr::str_extract(type_vehicle, "ONIBUS|MINIONBUS|MIDI|ONBUS|ONIBUS|MIDIONBUS|ONIB|ONIBUS|MIDI")) %>%
  # extract the euro
  mutate(euro = stringr::str_extract(euro, "EURO [:alpha:]")) %>%
  select(id_veiculo, vehicle_type, euro, fuel)

tb1_dia_veiculos <- tb1 %>%
  left_join(veiculos, by = c("id_veiculo")) %>%
  mutate(vehicle_type = ifelse(vehicle_type %in% c("ONBUS", "ONIB", "ONIBUS"), "ONIBUS", vehicle_type))

filter(tb1_dia_veiculos, is.na(vehicle_type)) %>% View()


# total vehicles per day
tb1_dia_veiculos %>% count(data, id_veiculo, vehicle_type) %>% count(data, vehicle_type) %>% 
  group_by(vehicle_type) %>% summarise(n = mean(n))

vkt_type <- tb1_dia_veiculos %>%
  group_by(vehicle_type) %>%
  summarise(vkt = sum(distancia_planejada), vehicles_n = n()) %>%
  # add brt
  rbind(data.frame(vehicle_type = "BRT", vkt = 66666 * 62, vehicles_n = NA)) %>%
  # daily
  mutate(vkt = vkt / 62, vehicles_n = vehicles_n / 62) %>%
  # yearly
  mutate(vkt = vkt * 365) %>%
  mutate(vkt_per_vehicle = vkt / vehicles_n)

# proportions
vkt_type_prop <- vkt_type %>%
  filter(!is.na(vehicle_type)) %>%
  mutate(vkt_prop = vkt / sum(vkt)) %>% select(vehicle_type, vkt_prop)

vkt_type_final <- vkt_type %>%
  left_join(vkt_type_prop, by = "vehicle_type") %>%
  # total to add to each category
  mutate(vkt_add = ifelse(is.na(vehicle_type), vkt, NA)) %>%
  tidyr::fill(vkt_add, .direction = "updown") %>%
  mutate(vkt_add = vkt_prop * vkt_add) %>%
  mutate(vkt = vkt + vkt_add) %>%
  filter(!is.na(vehicle_type)) %>%
  # reclassify vehicle type
  mutate(vehicle_type_c40 = ifelse(vehicle_type %in% c("MIDI", "MINIONBUS"), "MINIONIBUS", vehicle_type)) %>%
  group_by(vehicle_type_c40) %>%
  summarise(vkt = sum(vkt))
  

sum(vkt_type$vkt, na.rm = FALSE)
  