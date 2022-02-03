
getPuntosZona = function(datosDia,
                         ref,
                         listLineaProy,
                         puntList,
                         barrio,
                         varsJoin = c('ID_calle','NOM_CALLE','COD_NOMBRE','DESC_LINEA','DESC_VARIA','rangoF')) {
  
  
  
  #### Se queda con las paradas del bbarrio
  refBarrio = ref %>% st_set_geometry(NULL) %>% 
    filter(BARRIO %in% barrio) %>% 
    dplyr::select(COD_UBIC_P)
  
  ### Se queda con las filas que corresponden a las paradas de ese barrio
  datAgg = datosDia %>% filter(codigo_parada_bajada %in% refBarrio$COD_UBIC_P) 
  datAggFill = datAgg %>% group_by(ID) %>% slice_head()
  
  
##### Identifica la cantidad combinaciones a genera (coincide con el numero de filas)
  trials = 1:nrow(datAggFill)
  
##### Funcion que itera de forma paralela
  boot_fx = function(ii) {
    x = datAggFill[ii,]
    DESC_LINEA = as.character(x[,'DESC_LINEA'])
    codLinVar = as.character(x[,'sevar_codigo_subida'])
    parIni = as.numeric(x[,'codigo_parada_subida'])
    parFin = as.numeric(x[,'codigo_parada_bajada'])
    
    res = getSecuencia(codLinVar,parIni,parFin,listLineaProy,puntList)
    
    if(!is.null(unlist(res))) res$DESC_LINEA = DESC_LINEA
    
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
resff = datAgg %>% dplyr::select(ID,Tot,hora_Bajada) %>%
  group_by(hora_Bajada) %>%
  right_join(resFin) %>% 
  ungroup() %>% 
  st_as_sf(.,sf_column_name = 'geometry')


###### Construye la variable de la hora en rangos
resff = resff %>% mutate(rangoHoras = ifelse(hora_Bajada <= 5,1,
                                             ifelse(hora_Bajada > 5 & hora_Bajada <= 10,hora_Bajada - 4,
                                                    ifelse(hora_Bajada > 10 & hora_Bajada <= 15,7,
                                                           ifelse(hora_Bajada > 15 & hora_Bajada <= 19,hora_Bajada - 8,12)))),
                         rangoF = factor(rangoHoras,levels = 1:12,labels = c('0-5','6','7','8','9','10','11-15','16','17','18','19','20-23')))

##### Se queda con lospuntos unicos de la grilla y la geometria
# pointsUnic = resff %>% group_by(ID_calle,COD_NOMBRE,DESC_LINEA,DESC_VARIA,rangoF) %>% slice_head()
pointsUnic = resff %>% group_by_at(vars(dplyr::one_of(varsJoin))) %>% slice_head()

##### Cuenta la cantidad de viajes para cada punto y rango de hora
resFinAgg = resff %>% 
  st_set_geometry(NULL) %>%
  group_by_at(vars(dplyr::one_of(varsJoin))) %>%
  summarize(TotSeg = n(),
            SumTot = sum(Tot))


##### Agrega los totales a los puntos de la grilla
resPoint = pointsUnic %>% dplyr::select(dplyr::one_of(varsJoin)) %>%
  left_join(resFinAgg,by = c(varsJoin))

#######################
return(resPoint)
}
