
library(tidyverse)
library(here)
library(data.table)
library(sf)
library(htmlwidgets)
library(leaflet)
library(lubridate)

#########################################
#########################################
##### Cargo funciones 
fun = dir(here('Funciones'))
fun = fun[grep('\\.R',fun,ignore.case = T)]
for(ii in fun) source(here('Funciones',ii))


###################################
##### Cargo datos

load(here('Datos','bases_may_2018.RData'))
datos = valid
rm(valid,invalid)


###################################
##### Cargo Insumos
load('Resultados/Proy_Lineas_to_Calles.RData')
load('Resultados/Calles_Simplif.RData')

lineasUsar = names(listLineaProy)

#### Me quedo con los datos para las lineas variantes que tengo
datos = datos %>% dplyr::select(sevar_codigo_subida,
                         fecha_evento_subida,codigo_parada_subida,
                         fecha_evento_bajada,codigo_parada_bajada,
                         sentido_variante_subida) %>%
  filter(as.character(sevar_codigo_subida) %in% lineasUsar) %>%
  mutate(Fecha_Subida = as.POSIXct(fecha_evento_subida),
         Fecha_Bajada = as.POSIXct(fecha_evento_bajada),
         Dia = substr(fecha_evento_bajada,1,10),
         hora_Subida = hour(Fecha_Subida),
         min_Subida = minute(Fecha_Subida),
         hora_Bajada = hour(Fecha_Bajada),
         min_Bajada = minute(Fecha_Bajada))


#### Cobinas variante - subida y bajada

dia = '2018-05-23'

datosDia = datos %>% filter(Dia == dia) %>% 
  group_by(sentido_variante_subida,
           sevar_codigo_subida,codigo_parada_subida,
           codigo_parada_bajada,Dia,hora_Bajada) %>%
  summarise(Tot = n()) %>% 
  ungroup() %>% filter(Tot >= 0) 
   





###################################
##### Carga refugios
load('Resultados/refugio_sf.RData')

### Cargo el shape de zonas censales para luego asociarle barrios, segm y zonas a las paradas
# Zonas censales
ssz = st_read(here('SHP','Marco2011_ZONA_Montevideo_VivHogPer'))

### Hago el join espacial para pegarle la info a las paradas

ref = refugio %>% st_join(ssz[,c('CODSEC','CODSEG','CODCOMP','BARRIO','NROBARRIO','CCZ','MUNICIPIO')])

names(table(ref$BARRIO))
barrio = c("AGUADA")

refBarrio = ref %>% st_set_geometry(NULL) %>% 
  filter(BARRIO %in% barrio) %>% 
  dplyr::select(COD_UBIC_P)
###


##########################
##### En paralelo
# https://dept.stat.lsa.umich.edu/~jerrick/courses/stat701/notes/parallel.html
# http://gradientdescending.com/simple-parallel-processing-in-r/

datAgg = datosDia %>% filter(codigo_parada_bajada %in% refBarrio$COD_UBIC_P) %>% # & sentido_variante_subida == 'A' & hora_Bajada %in% c(7:9)
  mutate(ID = paste(sevar_codigo_subida,codigo_parada_subida,codigo_parada_bajada,sep = '_'))

chek = datAgg %>% group_by(hora_Bajada) %>% summarise(n = n())

datAggFill = datAgg %>% group_by(ID) %>% slice_head()
# datAggFill = datosDia %>%
#   mutate(ID = paste(sevar_codigo_subida,codigo_parada_subida,codigo_parada_bajada,sep = '_'))

library(parallel)

numCores = detectCores()

trials = 1:nrow(datAggFill)

boot_fx = function(ii) {
  x = datAggFill[ii,]
  codLinVar = as.character(x[,'sevar_codigo_subida'])
  parIni = as.numeric(x[,'codigo_parada_subida'])
  parFin = as.numeric(x[,'codigo_parada_bajada'])
  
  res = getSecuencia(codLinVar,parIni,parFin,listLineaProy,puntList)
  
  return(res)
}


cl <- makeCluster(detectCores())
clusterExport(cl, c("datAggFill","listLineaProy","puntList","getSecuencia"))
clusterEvalQ(cl, {
  library(tidyverse)
  library(sf)})

system.time({
  results <- parallel::parLapply(cl,trials,boot_fx)
})

stopCluster(cl)



names(results) = datAggFill$ID

##################################
#### GUardo RESULTS
save(results,file = 'Resultados/results.RData')


aa = do.call(c,lapply(results,is_empty))
sum(aa)

rr = results[!aa]

resFin = as.data.frame(data.table::rbindlist(rr)) 

# resFin = as.data.frame(data.table::rbindlist(rr)) %>%
#   left_join(datAggFill[,c('ID','Tot')]) %>% 
#   st_as_sf(.,sf_column_name = 'geometry') 
  

resff = datAgg %>% dplyr::select(ID,Tot,hora_Bajada) %>%
  group_by(hora_Bajada) %>%
  right_join(resFin) %>% 
  ungroup() %>% 
  st_as_sf(.,sf_column_name = 'geometry')

  


# pointsUnic = resFin %>% group_by(ID_calle,COD_NOMBRE) %>% slice_head()

# resFinAgg = resFin %>% group_by(ID_calle,NOM_CALLE,COD_NOMBRE) %>% 
#   st_set_geometry(NULL) %>%
#   summarise(TotSeg = n(),
#             SumTot = sum(Tot))


# resPoint = pointsUnic %>% left_join(resFinAgg,by = c('ID_calle','NOM_CALLE','COD_NOMBRE'))


resff = resff %>% mutate(rangoHoras = ifelse(hora_Bajada <= 5,1,
         ifelse(hora_Bajada > 5 & hora_Bajada <= 10,hora_Bajada - 4,
                ifelse(hora_Bajada > 10 & hora_Bajada <= 15,7,
                       ifelse(hora_Bajada > 15 & hora_Bajada <= 19,hora_Bajada - 8,12)))),
         rangoF = factor(rangoHoras,levels = 1:12,labels = c('0-5','6','7','8','9','10','11-15','16','17','18','19','20-23')))

pointsUnic = resff %>% group_by(ID_calle,COD_NOMBRE,rangoF) %>% slice_head()

resFinAgg = resff %>% 
  group_by(ID_calle,NOM_CALLE,COD_NOMBRE,rangoF) %>% 
  st_set_geometry(NULL) %>%
  summarise(TotSeg = n(),
            SumTot = sum(Tot))


resPoint = pointsUnic %>% left_join(resFinAgg,by = c('ID_calle','NOM_CALLE','COD_NOMBRE','rangoF'))

##################################
#### GUardo Spacial df con todos los puntos
save(resPoint,file = 'Resultados/resPoint.RData')


summary(resPoint$SumTot)


resPoint_lf = st_transform(x = resPoint, crs = 4326) %>% #filter(DESC_VARIA == 'A') %>%
  mutate(content = paste(sep = "<br/>",
                         paste("<b><a href='http://www.samurainoodle.com'>Flujo</a></b>:",SumTot)))

resPoint_lf$LogTot = round(log(resPoint_lf$SumTot))*5


ref_lf = st_transform(x = ref, crs = 4326)

# Barrio = ssz %>% filter(BARRIO == 'CIUDAD VIEJA')
# Barrio = st_transform(x = Barrio, crs = 4326)
#####

library(viridis)


pal <- colorNumeric(
  palette = 'Reds',
  domain = resPoint_lf$SumTot)

# pal = heat.colors(resPoint_lf$SumTot)

pp = leaflet() %>% # ABRE LA VENTANA PARA HACER EL MAPA
  addTiles(group = "OSM") %>% # DEFINE UN FONDO (POR DEFECTO OSM)
  addProviderTiles(providers$CartoDB.Positron, group = 'CartoDB.Positron') %>%
  # addCircles(data = lineas_lf,weight = 2,color = 'red',
  #            group = 'rutas') %>%
  addCircles(data = resPoint_lf,weight = 2,fill = TRUE,color = ~pal(resPoint_lf$SumTot),
             opacity = 0.5,
             fillOpacity = 0.5,
             radius=~LogTot,
             fillColor = ~pal(resPoint_lf$SumTot),
             popup=~content,
             group = 'lineas') %>%
  # addPolygons(data = Barrio,weight = 2,color = 'green',
  #              group = 'calles') %>%
  addLayersControl(
    baseGroups = c("CartoDB.Positron","OSM"),
    overlayGroups = c('rutas','lineas','calles'),
    options = layersControlOptions(collapsed = F)) 


#####################################
##### Para una lista de horas

resPoint_lf_split = split(resPoint_lf,resPoint_lf$rangoF)
tiposEmp = names(resPoint_lf_split)


Barrios = st_read('SHP/ine_barrios_mvd')
st_crs(Barrios) = 32721

Barrios_lf = st_transform(x = Barrios, crs = 4326)

pal <- colorBin(
  bins = 12,
  palette = 'Reds',
  domain = resPoint_lf$SumTot)


pp = leaflet() %>% # ABRE LA VENTANA PARA HACER EL MAPA
  addTiles(group = "OSM") %>% # DEFINE UN FONDO (POR DEFECTO OSM)
  addProviderTiles(providers$CartoDB.DarkMatter, group = 'CartoDB.Positron') %>%
  addPolygons(data = Barrios_lf %>% filter(NOMBBARR == 'AGUADA'),fill = TRUE,
                         color = 'blue',
                         opacity = 0.5,
                         fillOpacity = 0,
                         fillColor = 'blue')
  # addCircles(data = ref_lf,weight = 0.1,fill = TRUE,
  #            color = 'blue',
  #            opacity = 0.5,
  #            fillOpacity = 0.5,
  #            radius= 1,
  #            fillColor = 'blue',
  #            group = 'Paradas')

ppp = pp

tiposEmp %>%
  purrr::walk(function(df) {
    ppp <<- ppp %>% 
  addCircles(data = resPoint_lf_split[[df]],weight = 2,fill = TRUE,
             color = ~pal(resPoint_lf_split[[df]]$SumTot),
             opacity = 0.5,
             fillOpacity = 0.5,
             radius=~LogTot,
             fillColor = ~pal(resPoint_lf_split[[df]]$SumTot),
             popup=~content,
             group = df) %>% 
      addLegend(position = 'topleft',pal = pal,values = resPoint_lf_split[[df]]$SumTot,
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

