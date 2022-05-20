getSecuenciaAllLinVar = function(codLinVar,listLineaProy,puntList,paradas) {#),minSub = NULL,minBaj = NULL) {
  
  
  paradasLV = paradas %>% filter(COD_VARIAN == codLinVar) %>% select(COD_UBIC_P,ORDINAL) %>% arrange(ORDINAL) %>%
    mutate(content = paste(sep = "<br/>",
                           paste("<b><a>ORDINAL</a></b>:",ORDINAL),
                           paste("<b><a>COD_UBIC_P</a></b>:",COD_UBIC_P)))
  
  
  
  resPointCalle = listLineaProy[[as.character(codLinVar)]]
  
  posIni = max(1,min(which(paradasLV$COD_UBIC_P %in% resPointCalle$codParada)))
  posFin = min(nrow(paradasLV),max(which(paradasLV$COD_UBIC_P %in% resPointCalle$codParada)))
  
  parIni = paradasLV$COD_UBIC_P[posIni]
  parFin = paradasLV$COD_UBIC_P[posFin]
  
  
  
  
  DESC_VARIA = unique(resPointCalle$DESC_VARIA)
  ID = paste(codLinVar,parIni,parFin,sep = '_')
  
  Ind = sum(c(parIni,parFin) %in% resPointCalle$codParada) == 2
  
  if(Ind) {
    inputGet = resPointCalle[which(resPointCalle$codParada == parIni)[1]:(which(resPointCalle$codParada == parFin)[1]),] %>%
      group_by(NumCalle,COD_NOMBRE) %>%
      summarise(posIni = first(posIni),
                posFin = last(posFin)) %>% suppressMessages()
    
    
    res0 = do.call(rbind,sapply(1:nrow(inputGet),function(xx) {
      fila = inputGet[xx,]
      codCalle = as.character(fila$COD_NOMBRE)
      seqPos = fila$posIni : fila$posFin
      indRevert = fila$posIni > fila$posFin
      
      auxCalles = puntList[[codCalle]] %>% filter(ID_calle %in% seqPos) %>%
        mutate(direction = ifelse(indRevert, paste(dirFin,dirIni,sep = '_'),
               paste(dirIni,dirFin,sep = '_'))) %>% 
        cargaCoords(.) %>% st_set_geometry(NULL) %>%
        mutate(Xd = ifelse(direction == 'SUR_NORTE',X + 10,
                           ifelse(direction == 'NORTE_SUR',X - 10,X)),
               Yd = ifelse(direction == 'ESTE_OESTE',Y + 10,
                           ifelse(direction == 'OESTE_ESTE',Y-10,Y))) %>%
        st_as_sf(.,coords = c('Xd','Yd'),crs = 32721) 
      
      if(indRevert) auxCalles = auxCalles %>% arrange(desc(ID_calle))
      
      
      
      # %>%
      #   st_transform(., crs = 4326) %>%
      #   cargaCoords(.) %>%
      #   st_set_geometry(NULL)
      
      
      list(auxCalles)
    })
    )
    
    res0$DESC_VARIA = DESC_VARIA
    res0$COD_VARIAN = codLinVar
    res0$par_Sub = parIni
    res0$par_Baj = parFin
    res0$ID = ID
    res0$seqViaje = 1:nrow(res0)
    if(nrow(res0) > 1) res0 = res0[-nrow(res0),]
    res0 = res0 %>% group_by(ID_calle,COD_NOMBRE) %>% slice_head() %>% arrange(seqViaje)
    if(nrow(res0) > 1) {
      res0$pesoInt = (1:(nrow(res0))-1)/(nrow(res0)-1)
    } else {
      res0$pesoInt = 0
    }
    
    
    #####################################################
    ##### PEGA LA PARADA Y EL ORDINAL
    
    putNa0 = function(x) {
      x[is.na(x)] = 0
      return(x)
}
    
    rr = paradasLV %>% 
      st_join(res0, join=nngeo::st_nn, maxdist= Inf,k=1) %>% 
      select(COD_UBIC_P,ORDINAL,ID_calle,COD_NOMBRE) %>%
      st_set_geometry(NULL) %>%
      right_join(res0,by = c('ID_calle','COD_NOMBRE')) %>%
      arrange(seqViaje)  %>%
      suppressMessages()
    
    
    if(is.na(first(rr$ORDINAL)))  rr$ORDINAL[1] = 0
    
    
    rr = rr %>%
      mutate(dirAv = ifelse(direction == 'SUR_NORTE','S_N',
                            ifelse(direction == 'NORTE_SUR','N_S',
                                   ifelse(direction == 'ESTE_OESTE','E_O',
                                          ifelse(direction == 'OESTE_ESTE','O_E','')))),
             direction = factor(direction,levels = c('SUR_NORTE','NORTE_SUR','ESTE_OESTE','OESTE_ESTE')),
             ID_punto = paste(COD_NOMBRE,ID_calle,dirAv,sep = '_'),
             ORDINAL = fill.na(ORDINAL),
             across(c('COD_UBIC_P','ORDINAL'),putNa0)) %>%
      arrange(ORDINAL,seqViaje) %>% 
      mutate(Xl = lag(X),
             Yl = lag(Y),
             distAnt = sqrt((X-Xl)^2 + (Y-Yl)^2),
             distAnt = ifelse(is.na(distAnt),0,distAnt),
             seqViajeN = row_number()) %>%
      suppressMessages()
    
 
  } else {
    # res0 = list()
    rr = list()
  }
  
  # return(res0)
  return(rr)
}





############################################

# conProblemas: "24"


# rr = getSecuenciaAllLinVar(codLinVar = nn[6],listLineaProy,puntList,paradas)

# nn = lineasUsar
# codLinVar = nn[1]
# 
# rrr = rr %>% st_as_sf(.,coords = c('X','Y'),crs = 32721) %>%
#   st_transform(., crs = 4326) %>%
#   mutate(content = paste(sep = "<br/>",
#                          paste("<b><a>ORDINAL</a></b>:",ORDINAL),
#                          paste("<b><a>NOM_CALLE</a></b>:",NOM_CALLE),
#                          paste("<b><a>COD_NOMBRE</a></b>:",COD_NOMBRE),
#                          paste("<b><a>ID_calle</a></b>:",ID_calle),
#                          paste("<b><a>seqViaje</a></b>:",seqViaje)))
# #
# pp = leaflet() %>% # ABRE LA VENTANA PARA HACER EL MAPA
#   addTiles(group = "OSM") %>% # DEFINE UN FONDO (POR DEFECTO OSM)
#   addProviderTiles(providers$CartoDB.DarkMatter, group = 'CartoDB.Positron') %>%
#   addCircles(data = paradasLV %>% st_transform(., crs = 4326),weight = 1,fill = TRUE,
#              color = 'blue',
#              opacity = 0.5,
#              fillOpacity = 0.5,
#              radius= 100,
#              fillColor = 'green',
#              group = 'Paradas',
#              popup=~content) %>%
#   addCircles(data = rrr %>% st_transform(., crs = 4326),weight = 1,fill = TRUE,
#              color = 'red',
#              opacity = 0.5,
#              fillOpacity = 0.5,
#              radius= ~sqrt(seqViaje)*5,
#              fillColor = 'red',
#              group = 'Paradas',
#              popup=~content)# %>%
# 
# pp


#   
#   # addCircles(data = aux,weight = 1,fill = TRUE,
#   #            color = 'red',
#   #            opacity = 0.5,
#   #            fillOpacity = 0.5,
#   #            radius= 100,
#   #            fillColor = 'green',
#   #            group = 'Paradas') %>%
#   # 
#   # addCircles(data = aux0,weight = 1,fill = TRUE,
#   #            color = 'red',
#   #            opacity = 0.5,
#   #            fillOpacity = 0.5,
#   #            radius= 100,
#   #            fillColor = 'blue',
#   #            group = 'Paradas')
# 
# pp
# 
# 
