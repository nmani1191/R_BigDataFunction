# R Function to Import the data from HIVE tables to R environment as fast as possible. This function performs bettern than Rhive cran package

##################### Function Definition #######################
hive.import.table <- function (hive.database,hive.table,hive.cmd = "hive") 
{
  tmp.file <- sprintf("tmp_hadoop_%s.txt", as.numeric(Sys.time()))
  cmd <- sprintf("%s -e 'set hive.cli.print.header=true; SELECT * FROM %s.%s'  > %s", 
                 hive.cmd, hive.database, hive.table, tmp.file)
  system(command = cmd)
  df <- read.csv(tmp.file, header = T, stringsAsFactors = T,quote="",sep = "\t",na.strings = c("NA","na","NULL","null",""," "))
  file.remove(tmp.file)
  return(df)
}
#################################################################

##################### Function Testing ##########################

system.time(output <- hive.import.table("database_name","tablename"))
# Time taken: 1.755 seconds, Fetched: 344342 row(s)
# user     system elapsed 
# 24.279   2.684  22.911 
system.time(output1 <- hive.import.table("database_name","tablename1"))
# Time taken: 2.887 seconds, Fetched: 32449 row(s)
# user     system elapsed 
# 16.010   1.153  13.807
system.time(output2 <- hive.import.table("database_name","tablename2"))
# Time taken: 40.416 seconds, Fetched: 2796365 row(s)
# user     system elapsed 
# 617.108  20.695 674.075 
system.time(output3 <- hive.import.table("database_name","tablename3"))
# Time taken: 35.306 seconds, Fetched: 2225661 row(s)
# user     system elapsed 
# 491.191  23.823 543.877
#################################################################

data_summary <- function(input_dataset)
{
  df1 <- data.frame(character(),character(),integer(),integer(),integer(),numeric(),integer())
  
  for (name in colnames(input_dataset)) {
    
    df1 <- rbind(df1,data.frame(ColName=name,DataType=class(input_dataset[,name]),
                                TotalRecords=nrow(input_dataset),
                                UniqueCounts=length(unique(input_dataset[,name])),
                                UniquePercent=round(length(unique(input_dataset[,name]))/nrow(input_dataset)*100,2),
                                NACounts=sum(is.na(input_dataset[,name])),
                                NAPercent=round(sum(is.na(input_dataset[,name]))/nrow(input_dataset)*100,2),
                                RecordsExceed35CharLength=sum(nchar(as.character(iconv(input_dataset[,name], "latin1", "ASCII", sub=""))) >= 35,na.rm=T),
                                RecordsExceed35Percent = round(sum(nchar(as.character(iconv(input_dataset[,name], "latin1", "ASCII", sub=""))) >= 35,na.rm=T)/nrow(input_dataset)*100,2)))
    
  }
  df1$DataType <- as.character(df1$DataType)
  df1$FinalDataType <- ifelse(df1$UniquePercent > 40 & df1$RecordsExceed35Percent > 50,"Text",df1$DataType)
  
  df1 <- df1[order(df1$NACounts,decreasing = T),]
  
  return(df1)
}

before <- data_summary(hive_table_projectmaster)
after <- data_summary(hive_table_projectmaster)

