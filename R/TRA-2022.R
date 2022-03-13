rm(list=ls())

if (!require(devtools)) install.packages('devtools')
if (!require(needs)) install.packages('needs')
if (!require(Rccp)){
  install.packages("Rcpp")
  install.packages("RcppSimdJson")
  Sys.setenv("R_PROFILE"=file.path(getwd(), "Rprofile.site"))
  dotR <- file.path(Sys.getenv("HOME"), ".R")
  if (!file.exists(dotR)) 
    dir.create(dotR)
  M <- file.path(dotR, "Makevars.win")
  if (!file.exists(M)) 
    file.create(M)
  cat("CXX17 = g++-7 -std=gnu++17 -fPIC",
      file = M, sep = "\n", append = TRUE)
}

require(needs)
needs(tidytransit)
needs(lwgeom )
needs(dplyr)
needs(magrittr)
needs(sf)
needs(ggplot2)

dir <- file.path(getwd())

source(file.path(dir,"functions.R"))

local_gtfs_path = file.path(getwd(),"gtfs")
riga <- read_gtfs(file.path(local_gtfs_path,"gtfs_20191105.zip"))




calculate_route_frequency <- function(gtfs,day, start_time, end_time){
  gtfs <- set_servicepattern(gtfs)
  gtfs <- gtfs_as_sf(gtfs)
  gtfs$shapes$length <- st_length(gtfs$shapes)
  shape_lengths <- gtfs$shapes %>% as.data.frame() %>% select(shape_id, length, -geometry)
  patterns <- gtfs$.$dates_servicepatterns%>%filter(date==day)%>%pull(servicepattern_id)
  service_ids <- gtfs$.$servicepatterns%>%filter(servicepattern_id %in% patterns)%>%pull(service_id)
  am_stop_freq <- get_stop_frequency(gtfs, start_time = start_time, end_time = end_time, 
                                     service_ids = service_ids, by_route = TRUE)
  am_stop_freq <- am_stop_freq%>%
    group_by(route_id,stop_id,direction_id)%>%
    summarise(n_departures=sum(n_departures), mean_headway = (etime-stime)/sum(n_departures))
  
  route_frequency <-am_stop_freq%>%
    group_by(route_id)%>%
    summarise(total_departures = sum(n_departures), 
              median_headways = round(median(mean_headway)), 
              mean_headways = round(mean(mean_headway)), st_dev_headways = round(sd(mean_headway), 
                                                                                 2), stop_count = dplyr::n())
  return(route_frequency)
}

stime <- 6*3600
etime <- 10*3600
gtfs_old = read_gtfs(file.path(local_gtfs_path,"gtfs_20190302.zip"))
gtfs_new = read_gtfs(file.path(local_gtfs_path,"gtfs_20220201.zip"))
route_frequency_old <-calculate_route_frequency(gtfs_old, "2019-03-05", stime, etime)
route_frequency_new <-calculate_route_frequency(gtfs_new, "2022-02-01", stime, etime)

change <- route_frequency_new%>%left_join(route_frequency_old, by="route_id")%>%
  mutate(change_headways=(median_headways.x-median_headways.y)*100/median_headways.y,
         change_departures=(total_departures.x-total_departures.y)*100/total_departures.y)%>%
  select(route_id,change_headways,change_departures)
change

riga <- read_gtfs(file.path(local_gtfs_path,"gtfs_20220201.zip"))
gtfs <- set_servicepattern(riga)
gtfs <- gtfs_as_sf(gtfs)
patterns <- gtfs$.$dates_servicepatterns%>%filter(date=="2022-02-01")%>%pull(servicepattern_id)
service_ids <- gtfs$.$servicepatterns%>%filter(servicepattern_id %in% patterns)%>%pull(service_id)
routes_sf <- get_route_geometry(gtfs, service_ids = service_ids)
routes_sf <- routes_sf %>% inner_join(change, by = 'route_id') %>% 
  filter(!is.na(change_headways))%>%arrange(abs(change_headways))


routes_sf$route_id
routes_sf_crs <- sf::st_transform(routes_sf) 
routes_sf_crs %>% 
  ggplot() + 
  geom_sf(aes(colour=change_headways), size=2) + 
  scale_colour_gradient2(low="red",
                        mid = "lightgrey",
                        midpoint = 0,
                        high = "blue")+
  labs(color = "Change of headways, %") +
  theme_bw()


needs(rgdal)
needs(maptools)
needs(foreign)
needs(gpclib)
if (!require(gpclib)) install.packages("gpclib", type="source")
gpclibPermit()

shp <- readOGR(dsn = file.path(dir, "apkaimes", "Apkaimes.shp"))
shp@data = shp@data%>%filter()
plot(shp[grepl('RIG', shp$Code),], border="black", lwd=2)




needs(opentripplanner)  
require(RcppSimdJson)
path_data <- file.path(getwd(), "OTP")
otp_checks <- function(otp = otp, dir = dir, router = router, graph = FALSE, otp_version = otp_version){return(TRUE)}
rlang::env_unlock(env = asNamespace('opentripplanner'))
rlang::env_binding_unlock(env = asNamespace('opentripplanner'))
assign('otp_checks', otp_checks, envir = asNamespace('opentripplanner'))
rlang::env_binding_lock(env = asNamespace('opentripplanner'))
rlang::env_lock(asNamespace('opentripplanner'))

file.copy(file.path(local_gtfs_path,"gtfs_20220201.zip"),file.path(getwd(), "OTP","graphs","default","gtfs.zip"))

path_otp <- otp_dl_jar(path=path_data,cache=T)
log1 <- otp_build_graph(otp = path_otp, dir = path_data)
log2 <- otp_setup(otp = path_otp, dir = path_data)
otpcon <- otp_connect(timezone = "Europe/Helsinki")
route <- otp_plan(otpcon, date_time = as.POSIXct(strptime("2022-02-08 13:30", "%Y-%m-%d %H:%M")),
                  fromPlace = c(24.12085, 56.95366), 
                  toPlace = c(24.14205, 56.95255))
route
otp_stop()