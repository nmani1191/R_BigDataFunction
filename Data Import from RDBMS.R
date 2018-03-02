# R function to import data from RDBMS using SQOOP & HIVE hdoop services for fast retrival. it will import data as R Dataframe

sqoop_import_table <- function (sqoop_cmd, hive.cmd = "hive") 
{
  tmp.dir <- gsub("\\.","_",sprintf("tmp_hadoop_%s", as.numeric(Sys.time())))
  tmp.file <- sprintf("%s.txt",tmp.dir)
  
  sqoop_import_cmd <- sprintf("%s --hive-import --hive-drop-import-delims --hive-table  %s -m 1 --target-dir /%s",sqoop_cmd,tmp.dir,tmp.dir)
  system(command = sqoop_import_cmd)
  
  hive_query_cmd <- sprintf("%s -e \"set hive.cli.print.header=true; SELECT * FROM %s\"  > %s", hive.cmd, tmp.dir, tmp.file)
  system(command = hive_query_cmd)
  
  df <- read.csv(tmp.file, header = T, stringsAsFactors = T,quote="",sep = "\t",na.strings = c("NA","na","NULL","null",""," "))
  names(df) <- gsub("^.*::colsplit::","", sub("\\.","::colsplit::",names(df)))
  
  hive_drop_cmd <- sprintf("%s -e 'drop table %s'", hive.cmd,tmp.dir)
  system(command = hive_drop_cmd)
  file.remove(tmp.file)
  
  return(df)
}

# sqoop_cmd <- "sqoop import --connect \"jdbc:sqlserver://servername;database=dbname\" --username name --password pwd --query \"Select * from schemaname.tablename where \\$CONDITIONS\" --split-by \"uniquecolumn\" --fields-terminated-by \\t "

# exported 26 lakhs of records

#system.time( Output<- sqoop_import_table(sqoop_cmd))

#    user   system  elapsed 
# 777.297   48.996 1126.369

#Some column formating issue present in the above function, need to correct it.