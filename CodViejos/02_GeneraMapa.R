
library(tidyverse)
library(here)
library(data.table)
library(sf)
library(htmlwidgets)
library(leaflet)
library(lubridate)
library(parallel)
library(viridis)

#########################################
#########################################
##### Cargo funciones 
fun = dir(here('Funciones'))
fun = fun[grep('\\.R',fun,ignore.case = T)]
for(ii in fun) source(here('Funciones',ii))


###################################
##### Cargo datos
#### Cargo descripcion de las paradas
load('Resultados/ref.RData')
# load('Resultados/resPoint_long.RData')
# load('Resultados/resPoint_long_ID.RData')
# load('Resultados/resPoint_long_ID_Dir.RData')
load('Resultados/resPoint_long_ID_Dir02.RData')
###########################################
###########################################
###########################################
###########################################
####### MAPA LEAFLET

## Paradas
lineas = st_read(here("SHP","v_uptu_lsv","v_uptu_lsv.shp"))
st_crs(lineas) = 32721
lineas = st_transform(x = lineas, crs = 4326)


### Lineas simplificadas
load('Resultados/Lineas_Simplif.RData')
lRef = listLineas[['2652']] %>% st_transform(.,crs = 4326)
#####

load('Resultados/Calles_Simplif.RData')

# resPoint = listSegPoint[[1]]


# resPoint_filtro = resPoint %>% filter(BARRIO_sub %in% c('CIUDAD VIEJA'))

load('Resultados/linea_calles.RData')

unique(linea_calles[grep('san jose',linea_calles$CALLE,ignore.case = T),c('CALLE','COD_CALLE1')])

ccRef = puntList[['4956']] %>% st_transform(.,crs = 4326)
# resPoint_filtro = subset(resPoint,COD_NOMBRE %in% c(7572,7656,4143,3564,2061,918) & DESC_VARIA == 'A')

# auxID = subset(resPoint,COD_NOMBRE %in% c(7572) & DESC_VARIA == 'A')
# auxID = subset(resPoint,COD_NOMBRE %in% c(7572) & direction == 'OESTE_ESTE')
auxID = subset(resPoint,COD_NOMBRE %in% c(7572,2061,6300) &  direction == 'OESTE_ESTE')
# auxID = subset(resPoint,BARRIO_sub %in% 'POCITOS')
codID = unique(auxID$ID)

lineasRef = lineas %>% filter(DESC_LINEA %in% auxID$DESC_LINEA)

resPoint_filtro = subset(resPoint,ID %in% codID)

# resPoint_filtro = subset(resPoint,BARRIO_baj %in% c('CIUDAD VIEJA') & DESC_VARIA == 'A')
dim(resPoint_filtro)

resPoint_filtro$SumTot = resPoint_filtro$Tot

resPoint_filtro_agg = resPoint_filtro %>% group_by(ID_calle,NOM_CALLE,COD_NOMBRE,direction,hora_Subida) %>%
  summarise(X = mean(X),
            Y = mean(Y),
            SumTot = sum(SumTot),
            minPas = mean(minPas,na.rm = T)) %>%
  ungroup() %>%
  mutate(hora = floor(minPas/60),
         minProm = round(60*(minPas/60 - floor(minPas/60))),
         minProm = as.character(ifelse(nchar(minProm) == 1,paste0('0',minProm),minProm)),
         horaProm = paste(hora,minProm,sep = ':')
  )

# resPoint_filtro_unic = resPoint_filtro %>% group_by(ID_calle,NOM_CALLE,COD_NOMBRE,hora_Subida) %>%
#   slice_head() %>% dplyr::select(ID_calle,NOM_CALLE,COD_NOMBRE,hora_Subida)
# dim(resPoint_filtro_unic)
# 
# resPoint_filtro_agg = resPoint_filtro %>% st_set_geometry(NULL) %>%
#   group_by(ID_calle,NOM_CALLE,COD_NOMBRE,hora_Subida) %>%
#   summarise(SumTot = sum(SumTot)) %>% left_join(resPoint_filtro_unic,
#                                                 by = c('ID_calle','NOM_CALLE','COD_NOMBRE','hora_Subida')) %>%
#   st_as_sf(.,sf_column_name = 'geometry')

dim(resPoint_filtro_agg)  


# resPoint_lf = st_transform(x = resPoint_filtro_agg, crs = 4326) %>% #filter(DESC_VARIA == 'A') %>%
#   mutate(content = paste(sep = "<br/>",
#                          paste("<b><a href='http://www.samurainoodle.com'>Flujo</a></b>:",SumTot)))

# resPoint_filtro_agg$LogTot = round(log(resPoint_filtro_agg$SumTot))*5

resPoint_filtro_agg = resPoint_filtro_agg %>% mutate(LogTot = round(log(SumTot))*5,
                                                     content = paste(sep = "<br/>",
                          paste("<b><a href='http://www.samurainoodle.com'>Flujo punto</a></b>:",SumTot),
                          paste("<b><a href='http://www.samurainoodle.com'>Codigo calle</a></b>:",COD_NOMBRE),
                          paste("<b><a href='http://www.samurainoodle.com'>Codigo punto</a></b>:",ID_calle),
                          paste("<b><a href='http://www.samurainoodle.com'>Hora promedio</a></b>:",horaProm),
                          paste("<b><a href='http://www.samurainoodle.com'>Sentido</a></b>:",direction)))

save(resPoint_filtro_agg,file = 'Resultados/resPoint_filtro_agg.RData')

ref_lf = st_transform(x = ref, crs = 4326)

# ff_lf = st_transform(x = ff, crs = 4326)
#####################################
##### Para una lista de horas

resPoint_lf_split = split(resPoint_filtro_agg,resPoint_filtro_agg$hora_Subida)
tiposEmp = names(resPoint_lf_split)

# pal <- colorBin(
#   bins = 12,
#   palette = 'Reds',
#   domain = resPoint_lf$SumTot)


pp = leaflet() %>% # ABRE LA VENTANA PARA HACER EL MAPA
  addTiles(group = "OSM") %>% # DEFINE UN FONDO (POR DEFECTO OSM)
  addProviderTiles(providers$CartoDB.DarkMatter, group = 'CartoDB.Positron') %>%
  addCircles(data = ref_lf,weight = 0.1,fill = TRUE,
             color = 'blue',
             opacity = 0.5,
             fillOpacity = 0.5,
             radius= 0.1,
             fillColor = 'blue',
             group = 'Paradas')
  # addCircles(data = ref_lf %>% filter(COD_UBIC_P %in% c(3080,3082)),weight = 0.1,fill = TRUE,
  #            color = 'blue',
  #            opacity = 0.5,
  #            fillOpacity = 0.5,
  #            radius= 100,
  #            fillColor = 'blue',
  #            group = 'Paradas') %>%
  # addPolylines(data = lineasRef,weight = 2,color = 'darkgreen') %>%
  # addCircles(data = ccRef,weight = 2,fill = TRUE,
  #            color = 'blue',
  #            opacity = 1,
  #            fillOpacity = 1,
  #            radius= 1,
  #            fillColor = 'red',
  #            group = 'Calle') %>%
  # addCircles(data = lRef,weight = 0.4,fill = TRUE,
  #            color = 'pink',
  #            opacity = 1,
  #            fillOpacity = 1,
  #            radius= 1,
  #            fillColor = 'pink')
# addCircles(data = ff_lf,weight = 2,fill = TRUE,
#            color = 'red',
#            opacity = 0.5,
#            fillOpacity = 0.5,
#            radius= ~SumTot,
#            fillColor = 'red',
#            group = 'Paradas')

ppp = pp

tiposEmp %>%
  purrr::walk(function(df) {
    
    dat = resPoint_lf_split[[df]]
    
    pal <- colorBin(
      bins = 12,
      palette = 'Reds',
      domain = dat$SumTot)
    
    
    dat$op = 0.8 * dat$SumTot/max(dat$SumTot) + 0.2
    
    ppp <<- ppp %>% 
  addCircles(data = dat,lng = ~ X,lat = ~ Y,weight = 2,fill = TRUE,
             color = ~pal(dat$SumTot),
             opacity = ~op,
             fillOpacity = ~op,
             radius=~LogTot,
             fillColor = ~pal(dat$SumTot),
             popup=~content,
             group = df) %>% 
      addLegend(position = 'topleft',pal = pal,values = dat$SumTot,
                                  title = df,group = df)
  })


hideGrup = tiposEmp[tiposEmp != '7']

  ppp = ppp %>% 
    addLayersControl(
    baseGroups = c("CartoDB.Positron","OSM"),
    overlayGroups = c('Paradas',tiposEmp),
    options = layersControlOptions(collapsed = F)) %>%
    hideGroup(hideGrup) 

  
  ppp

