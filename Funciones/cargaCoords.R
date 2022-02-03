####################################################
####### EXTRAE LAS COORDENADA LAT Y LONG
cargaCoords = function(pointShp) {
  xy = data.frame(st_coordinates(pointShp))
  pointShp$X = xy$X
  pointShp$Y = xy$Y
  return(pointShp)
}

