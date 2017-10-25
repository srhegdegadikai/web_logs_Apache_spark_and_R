Web Log Analysis with R and Apache Spark
================
Suhas Hegde
2017-10-25

Web Log analysis -
------------------

The following R code were run on a single node Apache spark cluster to aggregate the required results. The process roughly follows the below mentioned sequence of steps.

-   Set up a Spark cluster back end
-   Read and Parse the Weblog file into the cluster based file system instead of memory(RAM)
-   Find cumulative frequency counts for each of the required parameters/columns("host", "request", "HTTP\_reply")

### Code -

``` r
# load the required libraries
library(readr)
library(dplyr)
library(ggplot2)
library(sparklyr)
library(magrittr)
library(DBI)

# configure the spark cluster
config <- spark_config()
config$spark.executor.cores <- 2
config$spark.executor.memory <- "4G"

# set up a local spark cluster
sc <- spark_connect(master = "local", config = config)

# create a vector to hold column names and another vector to hold the column
# types being read into the spark table
column_names <- c("host", "drop1", "drop2", "timestamp", "request", "HTTP_reply", 
    "reply_size")

column <- cols(host = "c", drop1 = "-", drop2 = "-", timestamp = "?", request = "c", 
    HTTP_reply = "i", reply_size = "i")

# read the log file using 'read_log' and then use 'sdf_copy_to' copy it to a
# spark table
read_log(paste(getwd(), "/NASA_access_log_Jul95", sep = ""), col_names = column_names, 
    col_types = column) %>% sdf_copy_to(sc, ., "web_log", overwrite = TRUE)
```

    ## # Source:   table<web_log> [?? x 5]
    ## # Database: spark_connection
    ##                    host                  timestamp
    ##                   <chr>                      <chr>
    ##  1         199.72.81.55 01/Jul/1995:00:00:01 -0400
    ##  2 unicomp6.unicomp.net 01/Jul/1995:00:00:06 -0400
    ##  3       199.120.110.21 01/Jul/1995:00:00:09 -0400
    ##  4   burger.letters.com 01/Jul/1995:00:00:11 -0400
    ##  5       199.120.110.21 01/Jul/1995:00:00:11 -0400
    ##  6   burger.letters.com 01/Jul/1995:00:00:12 -0400
    ##  7   burger.letters.com 01/Jul/1995:00:00:12 -0400
    ##  8      205.212.115.106 01/Jul/1995:00:00:12 -0400
    ##  9          d104.aa.net 01/Jul/1995:00:00:13 -0400
    ## 10       129.94.144.152 01/Jul/1995:00:00:13 -0400
    ## # ... with more rows, and 3 more variables: request <chr>,
    ## #   HTTP_reply <int>, reply_size <int>

``` r
src_tbls(sc)
```

    ## [1] "web_log"

``` r
# task 1 using 'host' as the key find out the cumulative frequency of each
# 'host'

tbl(sc, "web_log") %>% group_by(host) %>% summarise(freq = n()) %>% arrange(desc(freq))
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(freq)
    ##                    host  freq
    ##                   <chr> <dbl>
    ##  1 piweba3y.prodigy.com 17572
    ##  2 piweba4y.prodigy.com 11591
    ##  3 piweba1y.prodigy.com  9868
    ##  4   alyssa.prodigy.com  7852
    ##  5  siltb10.orl.mmc.com  7573
    ##  6 piweba2y.prodigy.com  5922
    ##  7   edams.ksc.nasa.gov  5434
    ##  8         163.206.89.4  4906
    ##  9          news.ti.com  4863
    ## 10 disarray.demon.co.uk  4353
    ## # ... with more rows

``` r
tbl(sc, "web_log") %>% group_by(host) %>% summarise(freq = n()) %>% top_n(n = 30, 
    wt = freq) %>% collect() %>% {
    (ggplot(data = .) + geom_bar(aes(host, freq), stat = "identity", fill = "red2", 
        alpha = 0.65) + coord_flip())
}
```

![](README_files/figure-markdown_github-ascii_identifiers/unnamed-chunk-1-1.png)

``` r
# task 2 using 'request' as the key find out the cumulative frequency of
# each 'request'

tbl(sc, "web_log") %>% group_by(request) %>% summarise(freq = n()) %>% arrange(desc(freq))
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(freq)
    ##                                      request   freq
    ##                                        <chr>  <dbl>
    ##  1   GET /images/NASA-logosmall.gif HTTP/1.0 110679
    ##  2    GET /images/KSC-logosmall.gif HTTP/1.0  89355
    ##  3 GET /images/MOSAIC-logosmall.gif HTTP/1.0  59967
    ##  4    GET /images/USA-logosmall.gif HTTP/1.0  59514
    ##  5  GET /images/WORLD-logosmall.gif HTTP/1.0  58997
    ##  6   GET /images/ksclogo-medium.gif HTTP/1.0  58411
    ##  7      GET /images/launch-logo.gif HTTP/1.0  40780
    ##  8          GET /shuttle/countdown/ HTTP/1.0  40132
    ##  9                    GET /ksc.html HTTP/1.0  39830
    ## 10     GET /images/ksclogosmall.gif HTTP/1.0  33528
    ## # ... with more rows

``` r
tbl(sc, "web_log") %>% group_by(request) %>% summarise(freq = n()) %>% top_n(n = 30, 
    wt = freq) %>% collect() %>% {
    (ggplot(data = .) + geom_bar(aes(request, freq), stat = "identity", fill = "palegreen4", 
        alpha = 0.65) + coord_flip())
}
```

![](README_files/figure-markdown_github-ascii_identifiers/unnamed-chunk-1-2.png)

``` r
# task 3 using 'HTTP_reply' as the key find out the cumulative frequency of
# each 'HTTP_reply'

tbl(sc, "web_log") %>% group_by(HTTP_reply) %>% summarise(freq = n()) %>% arrange(desc(freq))
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(freq)
    ##   HTTP_reply    freq
    ##        <int>   <dbl>
    ## 1        200 1701534
    ## 2        304  132627
    ## 3        302   46573
    ## 4        404   10833
    ## 5        500      62
    ## 6        403      53
    ## 7        501      14
    ## 8         NA      14
    ## 9        400       5

``` r
tbl(sc, "web_log") %>% group_by(HTTP_reply) %>% summarise(freq = n()) %>% collect() %>% 
    {
        (ggplot(data = .) + geom_bar(aes(as.character(HTTP_reply), freq), stat = "identity", 
            fill = "dodgerblue3", alpha = 0.65) + coord_flip())
    }
```

![](README_files/figure-markdown_github-ascii_identifiers/unnamed-chunk-1-3.png)

``` r
# Tasks using SQL task1
dbGetQuery(sc, "SELECT HOST, count(host) FREQUENCY FROM 
           web_log group by host order by count(host) desc  LIMIT 10")
```

    ##                    HOST FREQUENCY
    ## 1  piweba3y.prodigy.com     17572
    ## 2  piweba4y.prodigy.com     11591
    ## 3  piweba1y.prodigy.com      9868
    ## 4    alyssa.prodigy.com      7852
    ## 5   siltb10.orl.mmc.com      7573
    ## 6  piweba2y.prodigy.com      5922
    ## 7    edams.ksc.nasa.gov      5434
    ## 8          163.206.89.4      4906
    ## 9           news.ti.com      4863
    ## 10 disarray.demon.co.uk      4353

``` r
# task2
dbGetQuery(sc, "SELECT REQUEST, count(REQUEST) frequency FROM 
           web_log group by REQUEST order by count(REQUEST) desc  LIMIT 10")
```

    ##                                      REQUEST frequency
    ## 1    GET /images/NASA-logosmall.gif HTTP/1.0    110679
    ## 2     GET /images/KSC-logosmall.gif HTTP/1.0     89355
    ## 3  GET /images/MOSAIC-logosmall.gif HTTP/1.0     59967
    ## 4     GET /images/USA-logosmall.gif HTTP/1.0     59514
    ## 5   GET /images/WORLD-logosmall.gif HTTP/1.0     58997
    ## 6    GET /images/ksclogo-medium.gif HTTP/1.0     58411
    ## 7       GET /images/launch-logo.gif HTTP/1.0     40780
    ## 8           GET /shuttle/countdown/ HTTP/1.0     40132
    ## 9                     GET /ksc.html HTTP/1.0     39830
    ## 10     GET /images/ksclogosmall.gif HTTP/1.0     33528

``` r
# task3
dbGetQuery(sc, "SELECT HTTP_reply, count(HTTP_reply) frequency FROM 
                           web_log  where HTTP_reply = 404 group by HTTP_reply 
                                          order by count(HTTP_reply) desc LIMIT 10")
```

    ##   HTTP_reply frequency
    ## 1        404     10833

``` r
# disconnect from the cluster
spark_disconnect(sc)
```

References -
============

1.  ["sparklyr"](http://spark.rstudio.com) - R interface for Apache Spark - R studio
2.  ["NASA-HTTP"](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) - Two month's worth of all HTTP requests to the NASA Kennedy Space Center WWW server in Florida
