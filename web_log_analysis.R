library(readr)
library(dplyr)
library(ggplot2)
library(sparklyr)
library(magrittr)

# configuration for the spark cluster
config <- spark_config()
config$spark.executor.cores <- 2
config$spark.executor.memory <- "4G"

# set up a local spark cluster
sc <- spark_connect(master = "local", config = config)

#spark_connect(master = "yarn-client", version = "2.0.0") 

# for the given column names mention the object type being read
column <- cols(host ="c", drop1= "-", drop2 = "-", timestamp = "?",request = "c",
               HTTP_reply = "i", reply_size = "i")
# column names 
column_names <- c("host","drop1","drop2","timestamp","request",
                                          "HTTP_reply","reply_size")

# read the log file using "read_log" and then use "sdf_copy_to" 
# copy it to a spark table
read_log(paste(getwd(),"/NASA_access_log_Jul95", sep=""),col_names = column_names, 
         col_types = column) %>%
  sdf_copy_to(sc, ., "web_log", overwrite = TRUE)

src_tbls(sc)

# read the log file into the local environment
read_log(paste(getwd(),"/NASA_access_log_Jul95", sep=""),col_names = column_names, 
         col_types = column) -> web_log

# task 1
tbl(sc, "web_log") %>% group_by(host) %>% summarise(freq = n() ) %>% 
                                                            arrange(desc(freq))

tbl(sc, "web_log") %>% group_by(host) %>% summarise(freq = n() ) %>% 
  top_n(n = 30, wt = freq) %>% collect() %T>% 
  {print(ggplot(data=.) + 
        geom_bar(aes(host, freq) ,stat = "identity", fill="red2", alpha=.65) +
                                                                  coord_flip())}

# task 2
tbl(sc, "web_log") %>% group_by(request) %>% summarise(freq = n() ) %>% 
                                                          arrange(desc(freq))

tbl(sc, "web_log") %>% group_by(request) %>% summarise(freq = n() ) %>% 
  top_n(n = 30, wt = freq) %>% collect() %T>% 
  {print(ggplot(data=.) + 
    geom_bar(aes(request, freq) ,stat = "identity",fill="palegreen4", alpha=.65) + 
           coord_flip())}




# task 3
tbl(sc, "web_log") %>% group_by(HTTP_reply) %>% summarise(freq = n() ) %>% 
                                                      arrange(desc(freq))

tbl(sc, "web_log") %>% group_by(HTTP_reply) %>% summarise(freq = n() ) %>% 
   collect() %T>% 
  {print(ggplot(data=.) + 
      geom_bar(aes(as.character(HTTP_reply), freq) ,
               stat = "identity",fill="dodgerblue3", alpha=.65) +  coord_flip())}


# disconnect from the cluster
spark_disconnect(sc)

