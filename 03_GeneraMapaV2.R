
library(tidyverse)
library(here)
library(data.table)
library(sf)
library(htmlwidgets)
library(leaflet)
library(lubridate)
library(parallel)
library(viridis)

library(fst)
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
# ini = Sys.time()
# load('Resultados/resPoint_long_ID_Dir02.RData')
# fin = Sys.time()
# difftime(fin,ini)

ini = Sys.time()
resPoint = read.fst('Resultados/resPoint_long_ID_Dir02.fst')
fin = Sys.time()
difftime(fin,ini)

load('Resultados/ref.RData')
load('Resultados/puntRefUnicos.RData')
load('Resultados/codCallesUnic.RData')

###### Data frame nombre variables 

ref_df = ref %>% st_set_geometry(NULL)

###########################################
###########################################
###########################################
###########################################
####### MAPA LEAFLET

## Paradas
lineas = st_read(here("SHP","v_uptu_lsv","v_uptu_lsv.shp"))
st_crs(lineas) = 32721
lineas = st_transform(x = lineas, crs = 4326)


barrios = st_read(here('SHP','ine_barrios_mvd','ine_barrios_mvd.shp'))
barrios = barrios  %>% st_transform(.,crs = 4326)


ccz = st_read(here('SHP','sig_comunales','sig_comunales.shp'))
ccz = ccz  %>% st_transform(.,crs = 4326)

ccz$CCZ = as.character(as.numeric(gsub('CCZ','',ccz$ZONA_LEGAL)))


municipio = st_read(here('SHP','sig_municipios','sig_municipios.shp'))
municipio = municipio  %>% st_transform(.,crs = 4326)


### Lineas simplificadas
load('Resultados/Lineas_Simplif.RData')
lRef = listLineas[['2652']] %>% st_transform(.,crs = 4326)
#####

load('Resultados/Calles_Simplif.RData')

# resPoint = listSegPoint[[1]]


# resPoint_filtro = resPoint %>% filter(BARRIO_sub %in% c('CIUDAD VIEJA'))

load('Resultados/linea_calles.RData')

unique(linea_calles[grep('Rivera',linea_calles$CALLE,ignore.case = T),c('CALLE','COD_CALLE1')])

ccRef = puntList[['4956']] %>% st_transform(.,crs = 4326)
# resPoint_filtro = subset(resPoint,COD_NOMBRE %in% c(7572,7656,4143,3564,2061,918) & DESC_VARIA == 'A')

#### Horas intervalo
horaIni = 7
minIni = 30
horaFin = 10
minFin = 0

minPasIni = horaIni*60 + minIni
minPasFin = horaFin*60 + minFin


origenBarr =  NULL
destinoBarr = c('CENTRO','CIUDAD VIEJA')

origenCCZ = NULL
destinoCCZ = NULL

origenMuni = NULL
destinoMuni = NULL

codCallesUnic$NOM_CALLE[grep('agraciada',codCallesUnic$NOM_CALLE,ignore.case = T)]
# calle = 'AV 18 DE JULIO'
calle = NULL
sentido = c('N_S')

flujosEsp = TRUE


ref_df = ref_df %>% 
  mutate(CCZ = ifelse(nchar(CCZ) == 1,paste0('CCZ0',CCZ),
                                        paste0('CCZ',CCZ)))

ref_df_Melt = reshape2::melt(data = ref_df, 
     id.vars = c('COD_UBIC_P','COD_CALLE1','COD_CALLE2','CODSEC','CODSEG','CODCOMP'), 
     measure.vars = c('BARRIO','NROBARRIO','CCZ','MUNICIPIO'))

paradasOrig = c(origenBarr,origenCCZ,origenMuni)
paradasOrig = paradasOrig[!paradasOrig %in% "ALL"]

paradasDest = c(destinoBarr,destinoCCZ,destinoMuni)
paradasDest = paradasDest[!paradasDest %in% "ALL"]



indOrig = TRUE
if(!is.null(paradasOrig)) {
  ppar_Sub = subset(ref_df_Melt,value %in%  paradasOrig)
  ppar_Sub = unique(ppar_Sub$COD_UBIC_P)
} else {
  indOrig = FALSE
  ppar_Sub = NULL
}


indDest = TRUE
if(!is.null(paradasDest)) {
  ppar_Baj = subset(ref_df_Melt,value %in%  paradasDest)
  ppar_Baj = unique(ppar_Baj$COD_UBIC_P)
} else {
  indDest = FALSE
  ppar_Baj = NULL
}


# ppar_Sub = ref_df
# 
# if(!("ALL" %in% Barr_Sub  | is.null(Barr_Sub))) {
#   ppar_Sub = subset(ref_df,BARRIO %in%  Barr_Sub)
# }
# 
# 
# ppar_Baj = ref_df
# if(!("ALL" %in% Barr_Baj  | is.null(Barr_Baj))) {
#   ppar_Baj = subset(ref_df,BARRIO %in%  Barr_Baj)
# }


indCalle = TRUE
if(!("ALL" %in% calle  | is.null(calle))) {
  ppCalle = subset(puntRefUnicos,NOM_CALLE %in%  calle)
  if(!("ALL" %in% sentido  | is.null(sentido))) {
    ppCalle = subset(ppCalle,dirAv %in% sentido)$ID_punto
  }
} else {
  indCalle = FALSE
  ppCalle = NULL
}




resPoint = setDT(resPoint)
setkey(resPoint, ID_Unico)


auxID = resPoint


if(indOrig) {
  auxID = subset(auxID,par_Sub %in% ppar_Sub)
}

if(indDest) {
  auxID = subset(auxID,par_Baj %in% ppar_Baj)
}

if(indCalle) {
  auxID = subset(auxID,ID_punto %in% ppCalle)
}


auxID = subset(auxID,minPas >= minPasIni & minPas <= minPasFin)

# auxID = subset(resPoint,par_Sub %in% ppar_Sub$COD_UBIC_P & par_Baj %in% ppar_Baj$COD_UBIC_P & 
#                  ID_punto %in% ppCalle$ID_punto &
#                  minPas >= minPasIni & minPas <= minPasFin)
# 
# 
# auxID = subset(resPoint,ID_punto %in% ppCalle$ID_punto &
#                  minPas >= minPasIni & minPas <= minPasFin)



if(flujosEsp) {
  codID = unique(auxID$ID_Unico)
  # resPoint_filtro = subset(resPoint,ID_Unico %in% codID)
  resPoint_filtro = resPoint[.(codID), nomatch = 0L]
} else {
  resPoint_filtro = auxID
}
# codIDuni = auxID %>% select(ID,ID_Viaje) %>% unique()



# resPoint_filtro = resPoint[.(codID), nomatch = 0L]

# resPoint_filtro = resPoint %>% inner_join(codIDuni, by = c('ID','ID_Viaje'))

# resPoint_filtro = subset(resPoint,BARRIO_baj %in% c('CIUDAD VIEJA') & DESC_VARIA == 'A')
dim(resPoint_filtro)

resPoint_filtro$SumTot = resPoint_filtro$Tot

resPoint_filtro_agg = resPoint_filtro %>% group_by(ID_punto) %>%
  summarise(SumTot = sum(SumTot),
            minPas = mean(minPas,na.rm = T)) %>%
  ungroup() %>%
  mutate(hora = floor(minPas/60),
         minProm = round(60*(minPas/60 - floor(minPas/60))),
         minProm = as.character(ifelse(nchar(minProm) == 1,paste0('0',minProm),minProm)),
         horaProm = paste(hora,minProm,sep = ':')) %>%
  left_join(puntRefUnicos %>% dplyr::select(-dirAv))


dim(resPoint_filtro_agg)  


# resPoint_lf = st_transform(x = resPoint_filtro_agg, crs = 4326) %>% #filter(DESC_VARIA == 'A') %>%
#   mutate(content = paste(sep = "<br/>",
#                          paste("<b><a>Flujo</a></b>:",SumTot)))

# resPoint_filtro_agg$LogTot = round(log(resPoint_filtro_agg$SumTot))*5

resPoint_filtro_agg = resPoint_filtro_agg %>% mutate(LogTot = round(log(SumTot))*5,
                                                     content = paste(sep = "<br/>",
                          paste("<b><a>Flujo punto</a></b>:",SumTot),
                          paste("<b><a>Codigo calle</a></b>:",COD_NOMBRE),
                          paste("<b><a>Codigo punto</a></b>:",ID_calle),
                          paste("<b><a>Hora promedio</a></b>:",horaProm),
                          paste("<b><a>Sentido</a></b>:",direction)))

# save(resPoint_filtro_agg,file = 'Resultados/resPoint_filtro_agg.RData')

ref_lf = st_transform(x = ref, crs = 4326)

# ff_lf = st_transform(x = ff, crs = 4326)
#####################################
##### Para una lista de horas

# resPoint_lf_split = split(resPoint_filtro_agg,resPoint_filtro_agg$hora_Subida)
# tiposEmp = names(resPoint_lf_split)

# pal <- colorBin(
#   bins = 12,
#   palette = 'Reds',
#   domain = resPoint_lf$SumTot)

dat = resPoint_filtro_agg

pal <- colorBin(
  bins = 12,
  palette = 'Reds',
  domain = dat$SumTot)


dat$op = 0.8 * dat$SumTot/max(dat$SumTot) + 0.1



pp = leaflet() %>% # ABRE LA VENTANA PARA HACER EL MAPA
  addTiles(group = "OSM") %>% # DEFINE UN FONDO (POR DEFECTO OSM)
  addProviderTiles(providers$CartoDB.DarkMatter, group = 'CartoDB.Positron') %>%
  addCircles(data = ref_lf %>% filter(COD_UBIC_P %in% ppar_Sub),weight = 1,fill = TRUE,
             color = 'blue',
             opacity = 0.5,
             fillOpacity = 0.5,
             radius= 20,
             fillColor = 'green',
             group = 'Paradas') %>% 
  addCircles(data = ref_lf %>% filter(COD_UBIC_P %in% ppar_Baj),weight = 1,fill = TRUE,
             color = 'blue',
             opacity = 0.5,
             fillOpacity = 0.5,
             radius= 20,
             fillColor = 'blue',
             group = 'Paradas') %>% 
  addCircles(data = dat,lng = ~ X,lat = ~ Y,weight = 2,fill = TRUE,
             color = ~pal(dat$SumTot),
             opacity = ~op,
             fillOpacity = ~op,
             radius=~LogTot,
             fillColor = ~pal(dat$SumTot),
             popup=~content,
             group = 'Flujo') %>% 
      addLegend(position = 'topleft',pal = pal,values = dat$SumTot,
                                  title = 'Flujo',group = 'Flujo') %>% 
  
  addPolygons(data = barrios %>% dplyr::filter(NOMBBARR %in% c(origenBarr)),fillOpacity = 0,color = 'blue') %>%
  addPolygons(data = ccz %>% dplyr::filter(ZONA_LEGAL %in% c(origenCCZ)),fillOpacity = 0,color = 'blue') %>%
  addPolygons(data = municipio %>% dplyr::filter(MUNICIPIO %in% c(origenMuni)),fillOpacity = 0,color = 'blue') %>%
  
  addPolygons(data = barrios %>% dplyr::filter(NOMBBARR %in% c(destinoBarr)),fillOpacity = 0,color = 'green') %>%
  addPolygons(data = ccz %>% dplyr::filter(ZONA_LEGAL %in% c(destinoCCZ)),fillOpacity = 0,color = 'green') %>%
  addPolygons(data = municipio %>% dplyr::filter(MUNICIPIO %in% c(destinoMuni)),fillOpacity = 0,color = 'green') %>%
  
  # addPolygons(data = ccz,fillOpacity = 0) %>%
  
    addLayersControl(
    baseGroups = c("CartoDB.Positron","OSM"),
    overlayGroups = c('Flujos','Paradas'),
    options = layersControlOptions(collapsed = F)) %>%
    hideGroup('Paradas') 

  
  pp
  
  

