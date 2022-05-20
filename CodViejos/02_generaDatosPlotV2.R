
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
library(mefa)

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
# load('Resultados/refugio_sf.RData')

load('Resultados/ref.RData')

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
           codigo_parada_bajada,Dia,hora_Subida,hora_Bajada,timeVrec,minutos_sub,minutos_baj) %>% #,minutos_sub,minutos_baj) %>%
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
             "hora_Subida","hora_Bajada","timeVrec","minutos_sub","minutos_baj",
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
  datAgg = datosDia %>% filter(BARRIO_baj %in% ii) %>% mutate(ID_Viaje = row_number())
  # datAggFill = datAgg %>% group_by(ID,minutos_sub,minutos_baj) %>% slice_head()
  datAggFill = datAgg %>% group_by(ID) %>% slice_head()
  # datAggFill = datAgg
  # datAggFill$ID_viaje = paste(ii,1:nrow(datAggFill))
  
  
  ##### Identifica la cantidad combinaciones a genera (coincide con el numero de filas)
  trials = 1:nrow(datAggFill)
  
  ##### Funcion que itera de forma paralela
  boot_fx = function(jj) {
    x = datAggFill[jj,]
    DESC_LINEA = as.character(x[,'DESC_LINEA'])
    codLinVar = as.character(x[,'sevar_codigo_subida'])
    parIni = as.numeric(x[,'codigo_parada_subida'])
    parFin = as.numeric(x[,'codigo_parada_bajada'])
    # minSub = as.numeric(x[,'minutos_sub'])
    # minBaj = as.numeric(x[,'minutos_baj'])
    
    res = getSecuencia(codLinVar,parIni,parFin,listLineaProy,puntList)#,minSub,minBaj)
    
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
  
  # for(jj in trials) rr = boot_fx(jj)
  # ii = 'BUCEO'
  # jj = 7256
  
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
    group_by(hora_Bajada,hora_Subida,timeVrec,minutos_sub,minutos_baj) %>%
    right_join(resFin, by = 'ID') %>% 
    ungroup() %>% select(ID,ID_Viaje,seqViaje,hora_Subida,hora_Bajada,minutos_sub,minutos_baj,Tot,
                         ID_calle,NOM_CALLE,COD_NOMBRE,direction,X,Y,DESC_LINEA,
                         DESC_VARIA,COD_VARIAN,par_Sub,par_Baj,pesoInt) %>%
    mutate(minPas = minutos_sub + (minutos_baj - minutos_sub)*pesoInt)


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


save(listSegPoint,file = 'Resultados/listSegPointV2.RData')
###### Construye base con todos los datos

# listSegPoint = lapply(listSegPoint,function(xx) {
#   xx[,c("hora_Subida","hora_Bajada","timeVrec","Tot","ID",
#         "BARRIO_baj","BARRIO_sub","CCZ_sub","CCZ_baj","MUNICIPIO_sub","MUNICIPIO_baj",
#         "ID_calle","COD_NOMBRE",'NOM_CALLE',"X","Y","direction","DESC_VARIA","COD_VARIAN","par_Sub",
#         "par_Baj","DESC_LINEA","minutoPas")]
# })

# puntosAll = as.data.frame(data.table::rbindlist(listSegPoint))

resPoint = as.data.frame(data.table::rbindlist(listSegPoint)) 


resPoint = resPoint %>% 
  mutate(dirAv = ifelse(direction == 'SUR_NORTE','S_N',
                        ifelse(direction == 'NORTE_SUR','N_S',
                               ifelse(direction == 'ESTE_OESTE','E_O',
                                      ifelse(direction == 'OESTE_ESTE','O_E','')))),
         direction = factor(direction,levels = c('SUR_NORTE','NORTE_SUR','ESTE_OESTE','OESTE_ESTE')),
         ID_Unico = paste(ID,ID_Viaje,sep = '_'),
         ID_punto = paste(COD_NOMBRE,ID_calle,dirAv,sep = '_'))


################################################
###### Genero base con puntos unicos

puntRefUnicos = resPoint %>% 
  dplyr::select(ID_calle,NOM_CALLE,COD_NOMBRE,direction,X,Y,direction,dirAv,ID_punto) %>% 
  group_by(ID_calle,NOM_CALLE,COD_NOMBRE,direction) %>%
  slice_head() %>% ungroup()


##############################################
#### Saco variables de la base resPoint para que quede más manejable

resPoint = resPoint %>% dplyr::select(ID,ID_Unico,ID_punto,dirAv,minutos_sub,minutos_baj,Tot,
                                      DESC_LINEA,DESC_VARIA,COD_VARIAN,par_Sub,
                                      par_Baj,pesoInt,minPas)



#################################################
######## Codigos de calles
codCallesUnic = puntRefUnicos %>% dplyr::select(COD_NOMBRE,NOM_CALLE) %>% group_by(COD_NOMBRE,NOM_CALLE) %>%
  slice_head()


#################################################
#### Guardo los puntos de referencia unicos

save(codCallesUnic,file = 'Resultados/codCallesUnic.RData')

#################################################
#### Guardo los puntos de referencia unicos

save(puntRefUnicos,file = 'Resultados/puntRefUnicos.RData')

###########################################
###########################################
# save(resPoint,file = 'Resultados/resPoint_long.RData')
save(resPoint,file = 'Resultados/resPoint_long_ID_Dir02.RData')

write.fst(resPoint,'Resultados/resPoint_long_ID_Dir02.fst')
###########################################
###########################################
