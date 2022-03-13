load_complete_data <- function(data.complete.file, raw.folder,raw.rds.folder){
  if (!file.exists(data.complete.file)){
    data.complete<-tibble()
    unlink(raw.rds.folder, recursive=TRUE)
    dir.create(raw.rds.folder)
    c <- 1
    for (file in list.files(raw.folder)){
      print(file)
      tickets<-read_delim(paste0(raw.folder,file),";", col_types = "Tcccicccccc", 
                          col_names=c("datetime","tick","route_name", "route_direction","transport_id","ticket_type","mode","card_number","route_number","transport_code","code"))
      data.complete <- bind_rows(data.complete,tickets)
      c<-c+1
      if (c %% 10 == 0){
        saveRDS(data.complete, paste0(raw.rds.folder,"data",c,".rds"))
        data.complete<-tibble()
      }
    }
    saveRDS(data.complete, paste0(raw.rds.folder,"data",c,".rds"))
    
    data.complete<-tibble()
    for (file in list.files(raw.rds.folder, pattern="\\.rds$")){
      print(file)
      d<-readRDS(paste0(raw.rds.folder,file))
      data.complete <- bind_rows(data.complete,d)
    }
    data.complete%<>%mutate(date=ymd(format(datetime-3*60*60, format="%Y%m%d",tz="GMT")))
    data.complete%<>%mutate(mode=ifelse(mode=="Train" & as.integer(route_number)>100, "Urban bus", mode))
    data.complete%<>%filter(mode!="Train")
    data.complete%<>%mutate(mode=ifelse(mode=="Indeterminated", "Tramway", mode))
    
    mode_ass<-c(`Tramway`="tram", `Urban bus`="bus", `Trolley`="trol")
    data.complete$mode<-as.factor(data.complete$mode)
    data.complete%<>%mutate(route_id=paste("riga",mode_ass[as.character(mode)],route_number,sep="_"))
    
    dir_ass <- c(Back="b-a",Forth="a-b")
    data.complete%<>%mutate(shape_id=paste(route_id,dir_ass[route_direction],sep="_"))
    saveRDS(data.complete, data.complete.file)
  }else{
    data.complete<-readRDS(data.complete.file)
  }
  return(data.complete)
}


dt.haversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137){
  radians <- pi/180
  lat_to <- lat_to * radians
  lat_from <- lat_from * radians
  lon_to <- lon_to * radians
  lon_from <- lon_from * radians
  dLat <- (lat_to - lat_from)
  dLon <- (lon_to - lon_from)
  a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
  return(2 * atan2(sqrt(a), sqrt(1 - a)) * r)
}

get_gtfs_file <- function(gtfs.folder, day){
  fs <- gsub(".zip","",sort(list.files(gtfs.folder, pattern="\\.zip$",full.names=F),decreasing = T))
  day.d<-as.POSIXct(day,format="%Y-%m-%d",tz="UTC")
  res<-NULL
  for (file in fs){
    file.d<-as.POSIXct(file,format="%Y%m%d",tz="UTC")
    if (file.d<day.d){
      res<-file
      break
    }
  }
  return(res)
}


estimate_trips <- function(data){
  print("Extracting trips from ticket data...")
  data%<>%arrange(transport_code,datetime)%>%ungroup%>%mutate(trip_id=NA)%>%mutate(num.datetime=as.numeric(datetime))
  prev_code<-""
  prev_dir<-""
  prev_date<-NULL
  trip_id<-0
  f <- function(x) {
    code<-x["transport_code"]
    direct<-x["route_direction"]
    dt<-as.numeric(x["num.datetime"])
    if ((code!=prev_code) || (direct!=prev_dir) || is.null(prev_date) || (dt-prev_date)>20*60){
      trip_id <<-trip_id+1
    }
    prev_code <<- code
    prev_dir <<- direct
    prev_date <<- dt
    trip_id
  }
  data$trip_id<-apply(data, 1, f)
  data%<>%group_by(trip_id)%>%mutate(first_reg=min(datetime), last_reg=max(datetime))
  return(data)
}

#saveRDS(data.day, paste0(folder,"20180130-corr.rds"))
#data.day<-readRDS(paste0(folder,"20180130-corr.rds"))

#data.day%>%group_by(route_number, route_direction)




load_schedule <- function(gtfs.folder, day){
  print("Loading schedule...")
  gtfs.file<-paste0(gtfs.folder,get_gtfs_file(gtfs.folder,day),".zip")
  data <- import_gtfs(gtfs.file, local=T,quiet = T)
  
  data[['routes_df']] <- data[['routes_df']]%>%rename(route_id=1)
  data[['trips_df']] <- data[['trips_df']]%>%rename(route_id=1)
  data[['stops_df']] <- data[['stops_df']]%>%rename(stop_id=1)
  data[['stop_times_df']] <- data[['stop_times_df']]%>%rename(trip_id=1)
  data[['shapes_df']] <- data[['shapes_df']]%>%rename(shape_id=1)
  data[['calendar_df']] <- data[['calendar_df']]%>%rename(service_id=1)
  data[['calendar_dates_df']] <- data[['calendar_dates_df']]%>%rename(service_id=1)
  data[['agency_df']] <- data[['agency_df']]%>%rename(agency_id=1)%>%mutate(agency_name="Rigas Satiksme")
  return(data)
}


match_trips<-function(data.day, data, date.str){
  print("Matching extracted trips to scheduled...")
  trip.schedule <- data$trips_df%>%left_join(data$stop_times_df, by=c("trip_id"))%>%group_by(trip_id,shape_id)%>%summarise(first_stop=min(arrival_time), last_stop=max(departure_time))%>%arrange(shape_id, first_stop)
  trip.fact<-data.day%>%
                group_by(trip_id,shape_id)%>%summarise(first_reg=min(first_reg), last_reg=min(last_reg), reg_count=n())%>%filter(reg_count>1)%>%mutate(dur=(as.numeric(last_reg)-as.numeric(first_reg))/60)
  #trip.fact%<>%filter(dur<300)
  
  trip.schedule%<>%mutate(first_stop=as.POSIXct(paste(date.str,first_stop),format="%Y-%m-%d %H:%M:%S",tz="UTC"),
                          last_stop=as.POSIXct(paste(date.str,last_stop),format="%Y-%m-%d %H:%M:%S", tz="UTC"))
  res<-trip.fact%>%arrange(shape_id, first_reg)%>%left_join(trip.schedule%>%arrange(shape_id, first_stop), by=c("shape_id"))%>%filter(first_reg>first_stop,first_reg<(first_stop+10*60))#, last_reg<=(last_stop+20*60))
  res%<>%mutate(ddiff=abs(as.numeric(first_reg)-as.numeric(first_stop)))#+abs(as.numeric(last_reg)-as.numeric(last_stop)))
  res%<>%group_by(trip_id.x)%>%slice(which.min(ddiff))
  trip.match<-res%>%filter(!is.na(trip_id.y))%>%select(trip_id.x,trip_id.y)
  
  data.day%<>%group_by(card_number)%>%mutate(first_day=(datetime==min(datetime)))%>%
    left_join(trip.match, by=c("trip_id"="trip_id.x"))%>%filter(!is.na(trip_id.y))%>%ungroup%>%
    mutate(id=row_number())%>%left_join(data$stop_times_df,by=c("trip_id.y"="trip_id"))%>%
    mutate(arrival_time=as.POSIXct(paste(date.str,arrival_time),format="%Y-%m-%d %H:%M:%S",tz="UTC"))%>%
    mutate(ddiff=abs(as.numeric(datetime)-as.numeric(arrival_time)))%>%group_by(id)%>%slice(which.min(ddiff))%>%
    left_join(data$stops_df, by=c("stop_id"))
  
  # data.day3%>%filter(shape_id=="riga_bus_2_a-b")%>%select(id,datetime,trip_id, first_reg)%>%group_by(first_reg)%>%summarise(n=n(), tid=first(trip_id))
  # data.day3%>%filter(shape_id=="riga_bus_2_b-a")%>%select(id,datetime,trip_id, first_reg)%>%group_by(first_reg)%>%summarise(n=n(), tid=first(trip_id))%>%print(n=30)
  # data.day3%>%filter(trip_id==2960)%>%group_by(stop_name)%>%summarise(n=n(), atime=first(arrival_time))%>%arrange(atime)
  # data.day3%>%filter(trip_id==4721)%>%group_by(stop_name)%>%summarise(n=n(), atime=first(arrival_time))%>%arrange(atime)
  return(data.day)
}



find_chains <- function(data.day){
  print("Extracting chain trips...")
  data.day%<>%group_by(card_number)%>%mutate(reg_count=n())%>%filter(reg_count>1)%>%ungroup%>%arrange(card_number,datetime)%>%mutate(gap=ifelse(card_number==lag(card_number),(as.numeric(datetime)-as.numeric(lag(datetime)))/60,NA))
  chain_id<-0
  prev_card<-""
  f <- function(x) {
    gap<-x["gap"]
    card<-x["card_number"]
    if (prev_card!=card || is.na(gap) || as.numeric(gap)>60){
      chain_id <<-chain_id+1
    }
    prev_card<<-card
    chain_id
  }
  
  data.day$chain_id<-apply(data.day, 1, f)
  data.chains<-data.day%>%arrange(datetime)%>%group_by(card_number, chain_id)%>%summarise(chain_start=min(datetime), chain_end=max(datetime),start_stop_id=first(stop_id),last_stop_id=last(stop_id),first_day=any(first_day))
  return(data.chains)
}

find_mobility_vectors <- function(data.day){
  print("Constructing mobility vectors...")
  df <- data.day
  first_day<-NA
  prev_card<-""
  prev_stop<-""
  prev_last<-""
  prev_time<-""
  mobility<-list()
  cou<-0
  f<-function(x){
    cou<<-cou+1
    #if (cou %% 1000 == 0) print(cou)
    card<-x["card_number"]
    chain_start<-x["chain_start"]
    start<-x["start_stop_id"]
    last<-x["last_stop_id"]
    fd<-x["first_day"]
    if(card!=prev_card || cou==1){
      if (cou>1 && !is.na(first_day)){
        mobility[[length(mobility)+1]]<<-data.frame(card_number=prev_card,datetime=NA, start=prev_last, end=first_day)
      }
      if (fd){
        first_day<<-start
      } else{
        first_day<<-NA
      }
    }else{
      mobility[[length(mobility)+1]]<<-data.frame(card_number=card,datetime=prev_time, start=prev_stop, end=start)
    }
    if (cou==nrow(data.day)){
      if (start!=last) mobility[[length(mobility)+1]]<<-data.frame(card_number=card,datetime=chain_start, start=start, end=last)
      if (!is.na(first_day)) mobility[[length(mobility)+1]]<<-data.frame(card_number=card,datetime=NA, start=last, end=first_day)
    }
    prev_stop<<-start
    prev_time<<-chain_start
    prev_last<<-last
    prev_card<<-card
  }
  apply(data.day%>%arrange(card_number,chain_start), 1, f)
  mobility<-as_tibble(bind_rows(mobility))
  mobility%<>%mutate(datetime=as.POSIXct(datetime,format="%Y-%m-%d %H:%M:%S",tz="UTC"))
  mobility%<>%left_join(data$stops_df, by=c("start"="stop_id"))%>%left_join(data$stops_df, by=c("end"="stop_id"))%>%
    mutate(distance=dt.haversine(stop_lat.x,stop_lon.x,stop_lat.y,stop_lon.y))
  return(mobility )
}


construct_daily_mobility <- function(mobility, day,start_hours, vector_count, limit=0){
  print("Constructing mobility patterns...")
  t<-data.frame()
  
  for(st in start_hours){
    print(st)
    test<-tibble()
    if (is.na(st[1])){
      test<-mobility%>%filter(is.na(datetime))
    }else{
      from<-as.POSIXct(paste0(day," ",sprintf("%02d", st[1]),":00:00"),format="%Y-%m-%d %H:%M:%S",tz="UTC")
      to<-as.POSIXct(paste0(day," ",sprintf("%02d", st[2]),":00:00"),format="%Y-%m-%d %H:%M:%S",tz="UTC")
      test<-mobility%>%filter(datetime>from, datetime<to)
    }
    if (limit>0 && nrow(test)>limit) test%<>%sample_n(limit)
    print(paste("Size",nrow(test)))
    test%<>%
      mutate(X1=ifelse(is.na(datetime),rnorm(nrow(test),19*60,4*60),hour(datetime)*60+minute(datetime)))%>%
      mutate(X2=as.numeric(stop_lat.x))%>%
      mutate(X3=as.numeric(stop_lon.x))%>%
      mutate(X4=as.numeric(stop_lat.y))%>%
      mutate(X5=as.numeric(stop_lon.y))
    
    vect<-test%>%mutate(X1norm=(X1-mean(X1))/sd(X1))%>%
      mutate(X2norm=(X2-mean(X2))/sd(X2))%>%mutate(X3norm=(X3-mean(X3))/sd(X3))%>%mutate(X4norm=(X4-mean(X4))/sd(X4))%>%mutate(X5norm=(X5-mean(X5))/sd(X5))%>%
      select(X1norm,X2norm,X3norm,X4norm,X5norm)
    #d<-dist(vect, method = "euclidean")
    #hc1 <- hclust(d, method = "complete" )
    #test$cluster<-cutree(hc1, k=vector_count)
    
    #kc <- kmeans(d, vector_count)
    #test$cluster<-kc$cluster
    kc<-MiniBatchKmeans(vect,vector_count)
    test$cluster<-predict_KMeans(vect, kc$centroids)
    #Optimal_Clusters_GMM(vect,max_clusters = 50)
    
    t<-bind_rows(t,test%>%group_by(cluster)%>%
                   summarise(stop_lat.x=mean(stop_lat.x,na.rm=T),stop_lon.x=mean(stop_lon.x,na.rm=T),
                             stop_lat.y=mean(stop_lat.y,na.rm=T),stop_lon.y=mean(stop_lon.y,na.rm=T),
                             size=n(),
                             datetime=median(datetime)))
  }
  t%<>%mutate(cluster=row_number())
  return(t)
}

normalise_pattern <- function(mobility.pattern,norm.vals){
  #mutate(X1=ifelse(is.na(dayminutes),rnorm(nrow(mobility.pattern),19*60,4*60),as.numeric(dayminutes)))%>%
  mobility.pattern%>%
    mutate(dayminutes=hour(datetime)*60+minute(datetime))%>%
    mutate(X1=dayminutes)%>%
    mutate(X2=as.numeric(stop_lat.x))%>%
    mutate(X3=as.numeric(stop_lon.x))%>%
    mutate(X4=as.numeric(stop_lat.y))%>%
    mutate(X5=as.numeric(stop_lon.y))%>%
    mutate(X6=as.numeric(size))%>%
    mutate(X1norm=ifelse(is.na(X1),0,as.numeric((X1-norm.vals$dayminutes.mean)/norm.vals$dayminutes.sd)))%>%
    mutate(X2norm=(X2-norm.vals$stop_lat.x.mean)/norm.vals$stop_lat.x.sd)%>%
    mutate(X3norm=(X3-norm.vals$stop_lon.x.mean)/norm.vals$stop_lon.x.sd)%>%
    mutate(X4norm=(X4-norm.vals$stop_lat.y.mean)/norm.vals$stop_lat.y.sd)%>%
    mutate(X5norm=(X5-norm.vals$stop_lon.y.mean)/norm.vals$stop_lon.y.sd)%>%
    mutate(X6norm=(X6-norm.vals$size.mean)/norm.vals$size.sd)%>%
    select(X1norm,X2norm,X3norm,X4norm,X5norm,X6norm)
}

pattern_distance <- function(mobility.pattern1, mobility.pattern2,norm.vals,filter_size=0){
  p1<-mobility.pattern1
  if (filter_size>0) p1%<>%filter(size>filter_size)
  p2<-mobility.pattern2
  if (filter_size>0) p2%<>%filter(size>filter_size)
  #r1<-smint::closest(X=as.matrix(normalise_pattern(p1,norm.vals)),XNew=as.matrix(normalise_pattern(p2,norm.vals)))
  mdist<-flexclust::dist2(normalise_pattern(p1,norm.vals),normalise_pattern(p2,norm.vals), method="euclidean", p=2)
  soln<-RcppHungarian::HungarianSolver(mdist)
  #r2<-smint::closest(X=as.matrix(normalise_pattern(p1,norm.vals)),XNew=as.matrix(normalise_pattern(p2,norm.vals)))
  #return(min(sum(r1$dist),sum(r1$dist)))
  #return (sum(r1$dist))
  return(soln$cost)
}

