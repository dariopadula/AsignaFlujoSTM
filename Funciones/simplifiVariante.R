simplifiVariante = function(auxCalle,distSegm = 50,
                            varsKeep = c("COD_LINEA","DESC_LINEA","COD_VARIAN","DESC_VARIA"),
                            datAux = paradas, varFind = 'COD_VARIAN') {
  

  ### Se queda con el nombre y el codigo de la calle
  datCalle = auxCalle %>% st_set_geometry(NULL) %>% dplyr::select(one_of(varsKeep)) %>% unique()
  
  ### Agarro la info axiliar
  ddaux = datAux %>% inner_join(datCalle,by = varFind) %>% arrange(ORDINAL)
  
  ## Segmentiza la linea
  
  ddaux$tipo = 1
  
  df = data.frame(st_line_sample(auxCalle, density = 1/distSegm) %>% st_cast("POINT")) %>% 
    st_as_sf(.,sf_column_name = 'geometry',crs = 32721) %>% mutate(tipo = 0) %>% 
    rbind(.,ddaux %>% select(tipo)) 
  

  dist = st_distance(df,df)
  diag(dist) = Inf
  
  pos = 1
  permut = pos
  dist[,pos] = Inf
  
  cont = 1
  while(cont < nrow(dist)) {
    pos = which.min(dist[pos,])
    permut = c(permut,pos)
    dist[,pos] = Inf
    cont = cont + 1
  }
  
  ### Ordena los puntos aplicando la permutacion
  df_ord = df[permut,]
  
  
  df_ord = df_ord %>% mutate(ID_Lin = row_number())
  
  df_ord = cbind(df_ord,datCalle)
  
  ### Asigna el ordinal más proximo a cada punto de la linea
  matchPar<-st_join(df_ord, ddaux[,c('ORDINAL','COD_CALLE1')], join=nngeo::st_nn, maxdist= Inf,k=1) %>%
    suppressMessages()
  
  ### Indica si el orden de los puntos va en la misma direcion que los ordinales
  ### En caso que esto no pase se ordennan los puntos alreves
  IndCor = cor(matchPar$ID_Lin,matchPar$ORDINAL) < 0
  
  if(IndCor) {
    matchPar = matchPar %>% arrange(desc(ID_Lin)) %>% mutate(ID_Lin = row_number())
  }
  
  ### Itentifica los puntos de la linea mas cercanos a las paradas de la linea
  posMin = apply(st_distance(ddaux,df_ord),1,which.min)
  matchPar$codParada = 0
  matchPar$codParCalle = 0
  matchPar$codParada[posMin] = ddaux$COD_UBIC_P
  matchPar$codParCalle[posMin] = ddaux$COD_CALLE1
  return(matchPar)

}
