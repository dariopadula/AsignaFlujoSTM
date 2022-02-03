getCallesLimV2 = function(linaVar,
                        puntList) {
  
  ### Me capturo los codigos de calles de interes
  
  useCalle = linaVar %>% mutate(calleAnt = lead(COD_CALLE1),
                               callePos = lag(COD_CALLE1),
                               IndAnt = ifelse(calleAnt != COD_CALLE1,1,0),
                               IndPos = ifelse(callePos != COD_CALLE1,1,0),
                               Ind = ifelse(is.na(IndPos),IndAnt,IndPos),
                               NumCalle = cumsum(Ind)) %>% st_set_geometry(NULL) %>%
    group_by(NumCalle,COD_CALLE1) %>% slice_head() %>% select(NumCalle,COD_CALLE1) %>%
    data.frame() %>% filter(as.character(COD_CALLE1) %in% names(puntList))
  
  
  # useCalle = as.character(unique(linaVar$codParCalle))
  # useCalle = useCalle[useCalle != '0' & useCalle %in% names(puntList)]
  
  
  
  #### Me quedo con el grillado d epuntos de las calles
  # aux = do.call(rbind,puntList[useCalle])
  #### Hago el join espacial para encontrar los puntos de las clles mas cercanos a los de las linas
  match = do.call(rbind,
    sapply(1:nrow(useCalle),function(xx) {
    codCalle = as.character(useCalle[xx,'COD_CALLE1'])
    NumCalle = useCalle[xx,'NumCalle']
    aux = puntList[[codCalle]]
    
    linAux = subset(linaVar,COD_CALLE1 == codCalle)
    
    matchAux <- linAux %>% 
      st_join(aux, join=nngeo::st_nn, maxdist= Inf,k=1) %>% 
      suppressMessages()
    
    matchAux$NumCalle = NumCalle
    
    list(matchAux)
  })
  )
  
  # match<- linaVar %>% group_by(COD_CALLE1) %>%
  #   st_join(aux[,c('ID_calle','COD_NOMBRE','dirIni','dirFin')], join=nngeo::st_nn, maxdist= Inf,k=1)
  
  #### Determino secuencia de calles para capturar bien los segmentos en los casos que una calle 
  # aparezca mas de una vez
  
  # match = match %>%  
  #   mutate(calleAnt = lead(COD_NOMBRE),
  #          callePos = lag(COD_NOMBRE),
  #          IndAnt = ifelse(calleAnt != COD_NOMBRE,1,0),
  #          IndPos = ifelse(callePos != COD_NOMBRE,1,0),
  #          Ind = ifelse(is.na(IndPos),IndAnt,IndPos),
  #          NumCalle = cumsum(Ind),
  #          NumParada = cumsum(tipo))
  
  match = match %>%  
    mutate(NumParada = cumsum(tipo))
  
  #### Agrego la base anterir, y para cada secuencia de calle y numero de parada
  #### Capturo el punto de la calle inicial y el punto final de la calle
  resPointCalle = match %>% st_set_geometry(NULL) %>% group_by(NumCalle,COD_NOMBRE,NumParada) %>%
    summarise(posIni = first(ID_calle),
              posFin = last(ID_calle),
              COD_VARIAN = unique(COD_VARIAN),
              DESC_VARIA = unique(DESC_VARIA),
              codParada = max(codParada),
              ID_Lin = max(ID_Lin),
              dirIni = first(dirIni),
              dirFin = first(dirFin)) %>% suppressMessages()
  
  resPointCalle = resPointCalle %>% mutate(
    direction = ifelse(posIni > posFin, paste(dirFin,dirIni,sep = '_'),
                       paste(dirIni,dirFin,sep = '_')))
  
  return(resPointCalle)
}
