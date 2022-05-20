
##########################################################
#### Librerias
library(tidyverse)
library(here)

library(sf)
library(sp)
library(htmlwidgets)
library(leaflet)
library(nngeo)


#### Carpetas

if(!dir.exists('Funciones')) dir.create('Funciones')
if(!dir.exists('Resultados')) dir.create('Resultados')

#########################################
#########################################
##### Cargo funciones 
fun = dir(here('Funciones'))
fun = fun[grep('\\.R',fun,ignore.case = T)]
for(ii in fun) source(here('Funciones',ii))

######################################################
###### Cargo el shape de vias

vias = st_read(here("SHP","v_mdg_vias","v_mdg_vias.shp"))
st_crs(vias) = 32721

## Paradas
paradas = st_read(here("SHP","v_uptu_paradas","v_uptu_paradas.shp"))
st_crs(paradas) = 32721

##### Capturo linae variante y linea codigo

lineaCod = paradas %>% st_set_geometry(NULL) %>%
  dplyr::select(DESC_LINEA,COD_VARIAN) %>%
  unique()

save(lineaCod,file = 'Resultados/lineaCod.RData')

## Paradas
lineas = st_read(here("SHP","v_uptu_lsv","v_uptu_lsv.shp"))
st_crs(lineas) = 32721

######## Arma base con Calles, parada, linea, sentido

linea_calles = paradas %>% st_set_geometry(NULL) %>%
  left_join(lineas %>% st_set_geometry(NULL) %>% select(COD_VARIAN,DESC_VARIA), by = 'COD_VARIAN') %>%
  group_by(DESC_LINEA,DESC_VARIA,CALLE,COD_CALLE1) %>%
  slice_head() %>% ungroup() %>% select(DESC_LINEA,DESC_VARIA,CALLE,COD_CALLE1)


save(linea_calles,file = 'Resultados/linea_calles.RData')

###################################################
### Cargo el shape de zonas censales para luego asociarle barrios, segm y zonas a las paradas
# Zonas censales
ssz = st_read(here('SHP','Marco2011_ZONA_Montevideo_VivHogPer'))
############################################
###### Shape d paradas (refugios)
refugio = paradas %>% group_by(COD_UBIC_P) %>% 
  slice_head() %>% 
  dplyr::select(COD_UBIC_P,COD_CALLE1,COD_CALLE2)


### Hago el join espacial para pegarle la info a las paradas
ref = refugio %>% st_join(ssz[,c('CODSEC','CODSEG','CODCOMP','BARRIO','NROBARRIO','CCZ','MUNICIPIO')])
### Guardo los refugios con la info de las zonas
save(ref,file = 'Resultados/ref.RData')

#########################################
#########################################
#### Represento las calles de una forma simplificada

viasUsar = vias[vias$COD_NOMBRE  %in% paradas$COD_CALLE1,]
callesUsar = unique(viasUsar$COD_NOMBRE)

resList = list()
puntList = list()
cont = 0
for(ii in callesUsar) {
  cont = cont + 1
  if((cont %% 50) == 0) print(cont)
  
  auxCalle = vias %>% filter(COD_NOMBRE == ii)
  res = simplificaCalleV2(auxCalle,cellsizeMax = 500,npart = 10,distSegm = 75,
                        varsKeep = c('NOM_CALLE','COD_NOMBRE'))
  
  resList[[length(resList) + 1]] = res[[1]]
  puntList[[length(puntList) + 1]] = res[[2]]
}

names(resList) = as.character(callesUsar)
names(puntList) = as.character(callesUsar)

########################################
########################################
###### Representa las lineas variantes como puntos con las paradas identidficadas

lineasUsar = lineas %>% filter(!st_is_empty(st_geometry(.)) & COD_VARIAN %in% paradas$COD_VARIAN) %>%
  st_set_geometry(NULL)

listLineas = list()

# "7933" linea con problemas
cont = 0
for(ii in lineasUsar$COD_VARIAN) {
  
  cont = cont + 1
  if((cont %% 50) == 0) print(cont)
  
  auxCalle = lineas %>% filter(COD_VARIAN == ii)
  
  res = simplifiVarianteV2(auxCalle,distSegm = 25,
                         varsKeep = c("COD_LINEA","DESC_LINEA","COD_VARIAN","DESC_VARIA"),
                         datAux = paradas, varFind = 'COD_VARIAN')
  

  listLineas[[length(listLineas) + 1]] = res  
  
}

names(listLineas) = as.character(lineasUsar$COD_VARIAN)
##################################################
####### Proyecta las lineas sobre la nueva topologia de calles

listLineaProy = list()
cont = 0
for(ii in names(listLineas)) {
  
  cont = cont + 1
  if((cont %% 50) == 0) print(cont)
  
  listLineaProy[[length(listLineaProy) + 1]] = getCallesLimV2(linaVar = listLineas[[ii]],
                     puntList)
  
}

names(listLineaProy) = names(listLineas)


########################################
####### GUARDO LOS DATOS
# Representacion d ecalles en lineas y puntos
save(resList,puntList,file = 'Resultados/Calles_Simplif.RData')
# Lineas simplificadas
save(listLineas,file = 'Resultados/Lineas_Simplif.RData')
# Asignacion de puntos de la linea a los puntos de las calles
save(listLineaProy,file = 'Resultados/Proy_Lineas_to_Calles.RData')

