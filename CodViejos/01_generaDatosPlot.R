
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

##### Lo hago para un dia particular
dia = '2018-05-23'

###################################
##### Cargo datos
# Datos de un dia
load(here('Datos','datos_viajes.RData'))


###################################
##### Cargo Insumos
load('Resultados/Proy_Lineas_to_Calles.RData')
load('Resultados/Calles_Simplif.RData')

lineasUsar = names(listLineaProy)

###################################
##### Carga refugios
load('Resultados/refugio_sf.RData')

#### Cargo descripcion de las lineas
load('Resultados/lineaCod.RData')

# Crea variables de interes y agrupa 
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
  mutate(ID = paste(sevar_codigo_subida,codigo_parada_subida,codigo_parada_bajada,sep = '_')) %>% 
  inner_join(lineaCod, by = c('sevar_codigo_subida' = 'COD_VARIAN')) %>%
  left_join(ref %>% st_set_geometry(NULL) %>% dplyr::select(-COD_CALLE1,-COD_CALLE2),
            by = c('codigo_parada_bajada' = 'COD_UBIC_P')) %>%
  left_join(ref %>% st_set_geometry(NULL) %>% dplyr::select(-COD_CALLE1,-COD_CALLE2),
            by = c('codigo_parada_subida' = 'COD_UBIC_P'),suffix = c("_baj", "_sub"))
   

##### GUARDO LOS datos dias
# save(datosDia,file = 'Datos/datosDia.RData')
# load('Datos/datosDia.RData')


##### Variable de agrupacion 
varsJoin = c('ID','ID_calle','NOM_CALLE','COD_NOMBRE','DESC_LINEA','DESC_VARIA',
             "hora_Subida","hora_Bajada","timeVrec",
             "BARRIO_baj", "BARRIO_sub","direction")


#### Saca las geometrias y agrega coordenadas con crs lat long
# puntList = lapply(puntList,function(xx) {
#   yy = st_transform(x = xx, crs = 4326)
#   yy = cargaCoords(yy) %>% st_set_geometry(NULL)
#   
#   yy
# })


##### Para cada barrio de subida encuentra los puntos de las lias/variantes
##### Para cada viaje
listSegPoint = list()

### Data frame para cargar los tiempos y largos
nomBarrios = names(table(ref$BARRIO))
resdf = data.frame(nomBarrios,time = NA,filas = NA)
rownames(resdf) = nomBarrios

ccont = 0
for(ii in nomBarrios) {
  tini = Sys.time()  

  ### Se queda con las filas que corresponden a las paradas de ese barrio
  datAgg = datosDia %>% filter(BARRIO_baj %in% ii) 
  datAggFill = datAgg %>% group_by(ID) %>% slice_head()
  
  
  ##### Identifica la cantidad combinaciones a genera (coincide con el numero de filas)
  trials = 1:nrow(datAggFill)
  
  ##### Funcion que itera de forma paralela
  boot_fx = function(jj) {
    x = datAggFill[jj,]
    DESC_LINEA = as.character(x[,'DESC_LINEA'])
    codLinVar = as.character(x[,'sevar_codigo_subida'])
    parIni = as.numeric(x[,'codigo_parada_subida'])
    parFin = as.numeric(x[,'codigo_parada_bajada'])
    
    res = getSecuencia(codLinVar,parIni,parFin,listLineaProy,puntList)
    
    if(!is.null(unlist(res))) {
      res$DESC_LINEA = DESC_LINEA
    }
    
    return(res)
  }
  
  #### Detecta cores
  cl <- makeCluster(detectCores())
  #### Carga librerias e insumos  
  clusterExport(cl, c("datAggFill","listLineaProy","puntList","getSecuencia","cargaCoords"))
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
    group_by(hora_Bajada,hora_Subida,timeVrec) %>%
    right_join(resFin, by = 'ID') %>% 
    ungroup()


  ##### Cuenta la cantidad de viajes para cada punto y rango de hora
  # resAux = resff %>% 
  #   group_by_at(vars(dplyr::one_of(varsJoin))) %>%
  #   summarize(X = mean(X),
  #             Y = mean(Y),
  #             TotSeg = n(),
  #             SumTot = sum(Tot))
  
  
  ####################################################
  ####################################################
  resAux = resff
  
  listSegPoint[[length(listSegPoint) + 1]] = resAux
  
  tfin = Sys.time()
  ttot = round(as.numeric(difftime(tfin,tini,units = 'secs')))
  filas = nrow(resAux)
  resdf[ii,'time'] = ttot
  resdf[ii,'filas'] = filas
  
  ccont = ccont + 1 
  print(ccont)
  print(paste(ii,': ',ttot,' seg. Filas: ',filas))
}


save(listSegPoint,file = 'Resultados/listSegPoint.RData')
###### Construye base con todos los datos

listSegPoint = lapply(listSegPoint,function(xx) {
  xx[,c("hora_Subida","hora_Bajada","timeVrec","Tot","ID",
        "BARRIO_baj","BARRIO_sub","CCZ_sub","CCZ_baj","MUNICIPIO_sub","MUNICIPIO_baj",
        "ID_calle","COD_NOMBRE",'NOM_CALLE',"X","Y","direction","DESC_VARIA","COD_VARIAN","par_Sub",
        "par_Baj","DESC_LINEA")]
})

# puntosAll = as.data.frame(data.table::rbindlist(listSegPoint))

resPoint = as.data.frame(data.table::rbindlist(listSegPoint)) 

## Variable de agrupacion
# varsGroup = varsJoin

##### Agrego para calcular el flujo desagregado por las variables de grupo
# resPoint = puntosAll %>% group_by_at(vars(dplyr::one_of(varsGroup))) %>%
#   summarise(X = mean(X),
#             Y = mean(Y),
#             SumTot = sum(Tot)) %>% ungroup()



###########################################
###########################################
# save(resPoint,file = 'Resultados/resPoint_long.RData')
save(resPoint,file = 'Resultados/resPoint_long_ID_Dir.RData')
###########################################
###########################################
