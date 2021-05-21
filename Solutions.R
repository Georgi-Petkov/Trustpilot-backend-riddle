# Load sparklyr
library(sparklyr)
library(dplyr, warn.conflicts = FALSE)
library(stringr)
library(readr)
library(openssl)
library(ggplot2)
library(data.table)

library(parallel)
P <- detectCores(logical=FALSE)
P

# https://followthewhiterabbit.trustpilot.com/cs/step3.html - Link for the Trustpilot backend challenge

#The MD5 hash of the easiest secret phrase is "e4820b45d2277f3844eac66c903e84be"
#The MD5 hash of the more difficult secret phrase is "23170acc097c24edb98fc5488ab033fe"
#The MD5 hash of the hard secret phrase is "665e5bcb0c20062fe8abaaf4628bb154"

# Load the data 
words <- fread("C:\\Users\\2geor\\Desktop\\wordlist",header=FALSE,sep=",",stringsAsFactors=FALSE)
words <- read_csv("C:\\Users\\2geor\\Desktop\\wordlist",col_names =c('w1'))
# Check how it looks
head(words)

words_small <- words %>% 
  slice(grep("[^poultryoutwitsants]", w1, invert = TRUE)) %>% 
  distinct()

words_small %>% 
  mutate(word_len=str_length(w1)) %>% 
  filter(between(word_len,2,9)) %>% 
  ggplot(aes(word_len)) +
  geom_bar()


# Is it a data.table
is.data.table(words)

# Get the column names
names(words)

# Rename the variable
setnames(words,'V1','word1')

# Get only words with letters form the anagram
words_small <- words[!grep( "[^poultryoutwitsants]", word1)]

# Create a variable with the length of the words
words_small[,word_len:=str_length(word1)]


head(words_small)

dim(words_small)

df <- words_small[(words_small$word_len>1 & words_small$word_len<10),]

# Drop the column with the word length
df$word_len <- NULL

# Connect to your Spark cluster
sc <- spark_connect(master = "local")


# Copy df to Spark
df_tbl <- copy_to(sc, df,overwrite = TRUE)

# List the data frames available in Spark
src_tbls(sc)

# Link to the df table in Spark
df_tbl <- tbl(sc, "df",overwrite = TRUE )

# See how big the dataset is
dim(df_tbl)

# You need to glimpse at the table in Spark
glimpse(df_tbl)


# Cross join (in dplyr full_join) the table 3 times and look for the easiest phrase. Rename the variable on the go 
# it causes Spark to give an error otherwize
results <- df_tbl %>% 
  full_join(rename(df_tbl,c('word2'='word1')),by=character()) %>% 
  full_join(rename(df_tbl,c('word3'='word1')),by=character()) %>%
  transmute(
    str_len=nchar(paste0(word1,word2,word3)),
    string_alpha=paste(word1,word2,word3,sep = ' ')) %>%
  filter(str_len==18) %>% 
  mutate(strings = md5(string_alpha)) %>%
  filter(strings == "e4820b45d2277f3844eac66c903e84be" ) %>% 
  collect()

# Examine the class of the results
class(results)

# Print the result
results

# Cross join (in dplyr full_join) the table 3 times and look for the medium phrase
results2 <- df_tbl1 %>% 
  full_join(df_tbl2,by=character()) %>% 
  full_join(df_tbl3,by=character()) %>%
  transmute(
    str_len=nchar(paste0(word1,word2,word3)),
    string_alpha=paste(word1,word2,word3,sep = ' ')) %>%
  filter(str_len==18) %>% 
  mutate(strings = md5(string_alpha)) %>%
  filter(strings == "23170acc097c24edb98fc5488ab033fe" ) %>% 
  collect()




library(DBI)
dat <- dbGetQuery(spark_conn, "SELECT * FROM df CROSS JOIN df")
dat

l <- list(x=c('aa','bbb','c'))
df_test <- as_tibble(l)

df_test %>% 
  full_join(df_test,character()) %>% 
  rename('w1'=x.x,'w2'=x.y) %>% 
  full_join(df_test,character()) %>% 
  rename('w3'= x)%>% head()
transmute(
  str_len=nchar(paste0(x.x,x.y,x)),
  string_alpha=paste(x.x,x.y,x,sep = ' ')
) %>%
  #filter(str_len==18) %>% 
  mutate(strings=md5(string_alpha))


# Disconnect from Spark
spark_disconnect(sc= sc)


l <- list(z=c('aa','bbb','c'))
df_test3 <- as_tibble(l)



l <- list(x=c('aa','bbb','c'))
df_test2 <- as_tibble(l)
glimpse(df_tbl)

df_tbl %>%
  head() %>%
  full_join(rename(head(df_tbl),c('word2'='word1')),character()) %>% head()
rename('word1'=word1.x,'word2'=word1.y)
