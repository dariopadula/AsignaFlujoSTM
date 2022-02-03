
library(tidyverse)
library(here)
library(data.table)
library(sf)
library(htmlwidgets)
library(leaflet)
library(lubridate)
library(parallel)
library(viridis)

###################################
##### Cargo datos

# load(here('Datos','bases_may_2018.RData'))
# datos = valid
# rm(valid,invalid)

##### Lo hago para un dia particular
dia = '2018-05-23'

lineasUsar = names(listLineaProy)

#### Me quedo con los datos para las lineas variantes que tengo

datos = datos %>% mutate(Dia = substr(fecha_evento_bajada,1,10))

datos = subset(datos,Dia == dia)


datos = datos %>% dplyr::select(sevar_codigo_subida,
                                fecha_evento_subida,codigo_parada_subida,
                                fecha_evento_bajada,codigo_parada_bajada,
                                sentido_variante_subida) %>%
  filter(as.character(sevar_codigo_subida) %in% lineasUsar) %>%
  mutate(Fecha_Subida = as.POSIXct(fecha_evento_subida),
         Fecha_Bajada = as.POSIXct(fecha_evento_bajada),
         timeViaje = difftime(Fecha_Bajada,Fecha_Subida,units = "secs"),
         hora_Subida = hour(Fecha_Subida),
         min_Subida = minute(Fecha_Subida),
         hora_Bajada = hour(Fecha_Bajada),
         min_Bajada = minute(Fecha_Bajada))


#### GUardo los datos del dia
save(datos, file = here('Datos','datos_viajes.RData'))

