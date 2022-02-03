
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

# load(here('Datos','bases_may_2018.RData'))
# datos = valid
# rm(valid,invalid)


###################################
##### Cargo Insumos
load('Resultados/Proy_Lineas_to_Calles.RData')
load('Resultados/Calles_Simplif.RData')

lineasUsar = names(listLineaProy)

##### Lo hago para un dia particular
dia = '2018-05-23'

# datos = datos %>% mutate(Dia = substr(fecha_evento_bajada,1,10))
# datos = subset(datos, Dia == dia)
# #### Me quedo con los datos para las lineas variantes que tengo
# datos = datos %>% dplyr::select(sevar_codigo_subida,
#                          fecha_evento_subida,codigo_parada_subida,
#                          fecha_evento_bajada,codigo_parada_bajada,
#                          sentido_variante_subida) %>%
#   filter(as.character(sevar_codigo_subida) %in% lineasUsar) %>%
#   mutate(Fecha_Subida = as.POSIXct(fecha_evento_subida),
#          Fecha_Bajada = as.POSIXct(fecha_evento_bajada),
#          timeViaje = difftime(Fecha_Bajada,Fecha_Subida,units = "secs"),
#          Dia = substr(fecha_evento_bajada,1,10),
#          hora_Subida = hour(Fecha_Subida),
#          min_Subida = minute(Fecha_Subida),
#          hora_Bajada = hour(Fecha_Bajada),
#          min_Bajada = minute(Fecha_Bajada))
# 
# 
# save(datos,file = 'Datos/datos_viajes.RData')
#### Cobinas variante - subida y bajada

# 
# 
datosDia = datos %>% filter(Dia == dia) %>%
  mutate(timeVrec = ifelse(timeViaje <= 15*60,'0 a 15',
                           ifelse(timeViaje > 15*60 & timeViaje <= 30*60,'15 a 30',
                                  ifelse(timeViaje > 30*60 & timeViaje <= 60*60,'30 a 60','<60'))),
         minutos_sub = hora_Subida*60 + min_Subida,
         minutos_baj = hora_Bajada*60 + min_Bajada) %>%
  group_by(sentido_variante_subida,
           sevar_codigo_subida,codigo_parada_subida,
           codigo_parada_bajada,Dia,hora_Subida,hora_Bajada,timeVrec) %>% #,minutos_sub,minutos_baj) %>%
  summarise(Tot = n()) %>%
  ungroup() %>% filter(Tot >= 0) %>%
  mutate(ID = paste(sevar_codigo_subida,codigo_parada_subida,codigo_parada_bajada,sep = '_'))
   

###################################
##### Carga refugios
load('Resultados/refugio_sf.RData')

### Cargo el shape de zonas censales para luego asociarle barrios, segm y zonas a las paradas
# Zonas censales
ssz = st_read(here('SHP','Marco2011_ZONA_Montevideo_VivHogPer'))

### Hago el join espacial para pegarle la info a las paradas

ref = refugio %>% st_join(ssz[,c('CODSEC','CODSEG','CODCOMP','BARRIO','NROBARRIO','CCZ','MUNICIPIO')])

nomBarrios = names(table(ref$BARRIO))
# barrio = c("CIUDAD VIEJA","CENTRO")

resdf = data.frame(nomBarrios,time = NA,filas = NA)
rownames(resdf) = nomBarrios


#### Cargo descripcion de las lineas
load('Resultados/lineaCod.RData')

#### Agrego la descripción de la linea 
# datosDia = datosDia %>% inner_join(lineaCod %>% dplyr::select(-DESC_LINEA), by = c('sevar_codigo_subida' = 'COD_VARIAN'))
datosDia = datosDia %>% inner_join(lineaCod, by = c('sevar_codigo_subida' = 'COD_VARIAN'))

######################################
##### Agrego el barrio de llegada y el barrio de subida

datosDia = datosDia %>%
  left_join(ref %>% st_set_geometry(NULL) %>% dplyr::select(-COD_CALLE1,-COD_CALLE2),
            by = c('codigo_parada_bajada' = 'COD_UBIC_P')) %>%
  left_join(ref %>% st_set_geometry(NULL) %>% dplyr::select(-COD_CALLE1,-COD_CALLE2),
            by = c('codigo_parada_subida' = 'COD_UBIC_P'),suffix = c("_baj", "_sub"))


##### GUARDO LOS datos dias
# save(datosDia,file = 'Datos/datosDia.RData')
# load('Datos/datosDia.RData')

varsJoin = c('ID_calle','NOM_CALLE','COD_NOMBRE','DESC_LINEA','DESC_VARIA',
             "hora_Subida","hora_Bajada","timeVrec",
             "BARRIO_baj", "BARRIO_sub")


# puntList = lapply(puntList,function(xx) {
#   yy = st_transform(x = xx, crs = 4326)
#   yy = cargaCoords(yy) %>% st_set_geometry(NULL)
#   
#   yy
# })

listSegPoint = list()
for(ii in nomBarrios) {
  tini = Sys.time()  
  
  # varsJoin = c('ID_calle','NOM_CALLE','COD_NOMBRE','DESC_LINEA','DESC_VARIA',
  #              "hora_Subida","hora_Bajada","minutos_sub","minutos_baj","timeVrec", 
  #              "BARRIO_baj", "BARRIO_sub")
  
  

  
  ### Se queda con las filas que corresponden a las paradas de ese barrio
  # datAgg = datosDia %>% filter(codigo_parada_bajada %in% refBarrio$COD_UBIC_P)
  datAgg = datosDia %>% filter(BARRIO_baj %in% ii) 
  datAggFill = datAgg %>% group_by(ID) %>% slice_head()
  
  
  ##### Identifica la cantidad combinaciones a genera (coincide con el numero de filas)
  trials = 1:nrow(datAggFill)
  
  ##### Funcion que itera de forma paralela
  boot_fx = function(jj) {
    x = datAggFill[jj,]
    DESC_LINEA = as.character(x[,'DESC_LINEA'])
    # BARRIO_baj = as.character(x[,'BARRIO_baj'])
    # BARRIO_sub = as.character(x[,'BARRIO_sub'])
    codLinVar = as.character(x[,'sevar_codigo_subida'])
    parIni = as.numeric(x[,'codigo_parada_subida'])
    parFin = as.numeric(x[,'codigo_parada_bajada'])
    
    res = getSecuencia(codLinVar,parIni,parFin,listLineaProy,puntList)
    
    if(!is.null(unlist(res))) {
      res$DESC_LINEA = DESC_LINEA
      # res$BARRIO_baj = BARRIO_baj
      # res$BARRIO_sub = BARRIO_sub
    }
    
    return(res)
  }
  
  #### Detecta cores
  cl <- makeCluster(detectCores())
  #### Carga librerias e insumos  
  clusterExport(cl, c("datAggFill","listLineaProy","puntList","getSecuencia"))
  clusterEvalQ(cl, {
    library(tidyverse)
    library(sf)})
  
  ### Corre en paralelo   
  system.time({
    results <- parallel::parLapply(cl,trials,boot_fx)
  })
  
  
  ### Cierra los clusters
  stopCluster(cl)
  
  ### Le pone el nombre del ID
  names(results) = datAggFill$ID
  ### Detecta vacias
  vacias = do.call(c,lapply(results,is_empty))
  ### Se queda con las no vacias
  res = results[!vacias]
  ### Genera el data frame con todas
  resFin = as.data.frame(data.table::rbindlist(res)) 
  
  ### A los datos agregados por lina variante/subida/bajada le agrega los puntos de la grilla
  ### de calles
  resff = datAgg %>% dplyr::select(-sentido_variante_subida,-sevar_codigo_subida,
                                   -codigo_parada_subida,-codigo_parada_bajada,-Dia,-DESC_LINEA) %>%
    # group_by(hora_Bajada,hora_Subida,timeVrec,minutos_sub,minutos_baj) %>%
    group_by(hora_Bajada,hora_Subida,timeVrec) %>%
    right_join(resFin, by = 'ID') %>% 
    ungroup() # %>% 
  #   st_as_sf(.,sf_column_name = 'geometry')

  
  
  ##### Se queda con lospuntos unicos de la grilla y la geometria
  # pointsUnic = resff %>% group_by(ID_calle,COD_NOMBRE,DESC_LINEA,DESC_VARIA,rangoF) %>% slice_head()
  # pointsUnic = resff %>% group_by_at(vars(dplyr::one_of(varsJoin))) %>% slice_head()
  
  ##### Cuenta la cantidad de viajes para cada punto y rango de hora
  resFinAgg = resff %>% 
    # st_set_geometry(NULL) %>%
    group_by_at(vars(dplyr::one_of(varsJoin))) %>%
    summarize(X = mean(X),
              Y = mean(Y),
              TotSeg = n(),
              SumTot = sum(Tot))
  
  
  ##### Agrega los totales a los puntos de la grilla
  # resAux = pointsUnic %>% dplyr::select(dplyr::one_of(varsJoin)) %>%
  #   left_join(resFinAgg,by = c(varsJoin))
  
  resAux = resFinAgg
  ####################################################
  ####################################################
  
  listSegPoint[[length(listSegPoint) + 1]] = resAux  
  
  tfin = Sys.time()
  ttot = round(as.numeric(difftime(tfin,tini,units = 'secs')))
  filas = nrow(resAux)
  resdf[ii,'time'] = ttot
  resdf[ii,'filas'] = filas
  
  print(paste(ii,': ',ttot,' seg. Filas: ',filas))
}


# save(listSegPoint,file = 'Resultados/listSegPoint.RData')
###### Construye base con todos los datos

puntosAll = as.data.frame(data.table::rbindlist(listSegPoint)) 

# varsGroup = c('ID_calle','NOM_CALLE','COD_NOMBRE',"hora_Subida","hora_Bajada",
#               "minutos_sub","minutos_baj","timeVrec","BARRIO_baj","BARRIO_sub")

varsGroup = varsJoin

# puntosAll_unicos = puntosAll %>% group_by_at(vars(dplyr::one_of(varsGroup))) %>%
#   slice_head() %>% 
#   dplyr::select(dplyr::one_of(c(varsGroup,'geometry')))

puntosAll_agg = puntosAll %>% group_by_at(vars(dplyr::one_of(varsGroup))) %>%
  summarise(X = mean(X),
            Y = mean(Y),
            SumTot = sum(SumTot)) %>% ungroup()


# resPoint = puntosAll_unicos %>% right_join(puntosAll_agg, by = varsGroup) %>%
#   st_as_sf(.,sf_column_name = 'geometry') %>% st_transform(x = ., crs = 4326)

resPoint = puntosAll_agg
# resPoint = resPoint %>% st_transform(x = ., crs = 4326)

# resPoint = cargaCoords(resPoint) %>% st_set_geometry(NULL)
# resPoint = resPoint %>% st_set_geometry(NULL)



save(resPoint,file = 'Resultados/resPoint_long.RData')
###########################################
###########################################
###########################################
###########################################
####### MAPA LEAFLET
