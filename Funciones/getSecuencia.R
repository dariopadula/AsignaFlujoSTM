getSecuencia = function(codLinVar,parIni,parFin,listLineaProy,puntList) {#),minSub = NULL,minBaj = NULL) {
  
  resPointCalle = listLineaProy[[as.character(codLinVar)]]
  
  
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
        st_as_sf(.,coords = c('Xd','Yd'),crs = 32721) %>%
        st_transform(., crs = 4326) %>%
        cargaCoords(.) %>%
        st_set_geometry(NULL)
      
      
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
    
    
    
    
    ### Interpola los minutos
    # if(!is.null(minSub)) {
    # 
    #   difMin =  minBaj - minSub 
    #   res0$pesoInt = (1:(nrow(res0))-1)/(nrow(res0)-1)
    # } else {
    #   res0$minutoPas = NA
    # }
     
    
  } else {
    res0 = list()
  }
  
  return(res0)
}
