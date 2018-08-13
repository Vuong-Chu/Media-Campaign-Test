#
# prior preparation
#
## erase variables
rm(list = ls())


## add packages
is.exist <- "bit64" %in% rownames(installed.packages())
if(!is.exist){install.packages("bit64")}
is.exist <- "car" %in% rownames(installed.packages())
if(!is.exist){install.packages("car", dependencies=TRUE)}
is.exist <- "chron" %in% rownames(installed.packages())
if(!is.exist){install.packages("chron", dependencies=TRUE)}
is.exist <- "compute.es" %in% rownames(installed.packages())
if(!is.exist){install.packages("compute.es", dependencies=TRUE)}
is.exist <- "data.table" %in% rownames(installed.packages())
if(!is.exist){install.packages("data.table")}
is.exist <- "doParallel" %in% rownames(installed.packages())
if(!is.exist){install.packages("doParallel", dependencies=TRUE)}
is.exist <- "dplyr" %in% rownames(installed.packages())
if(!is.exist){install.packages("dplyr")}
is.exist <- "dummies" %in% rownames(installed.packages())
if(!is.exist){install.packages("dummies", dependencies=TRUE)}
is.exist <- "foreach" %in% rownames(installed.packages())
if(!is.exist){install.packages("foreach", dependencies=TRUE)}
is.exist <- "GGally" %in% rownames(installed.packages())
if(!is.exist){install.packages("GGally", dependencies=TRUE)}
is.exist <- "ggmcmc" %in% rownames(installed.packages())
if(!is.exist){install.packages("ggmcmc", dependencies=TRUE)}
is.exist <- "ggplot2" %in% rownames(installed.packages())
if(!is.exist){install.packages("ggplot2", dependencies=TRUE)}
is.exist <- "lme4" %in% rownames(installed.packages())
if(!is.exist){install.packages("lme4", dependencies=TRUE)}
is.exist <- "psych" %in% rownames(installed.packages())
if(!is.exist){install.packages("psych", dependencies=TRUE)}
is.exist <- "pwr" %in% rownames(installed.packages())
if(!is.exist){install.packages("pwr", dependencies=TRUE)}
is.exist <- "ranger" %in% rownames(installed.packages())
if(!is.exist){install.packages("ranger", dependencies=TRUE)}
is.exist <- "RCurl" %in% rownames(installed.packages())
if(!is.exist){install.packages("RCurl", dependencies=TRUE)}
is.exist <- "rstan" %in% rownames(installed.packages())
if(!is.exist){install.packages("rstan", dependencies=TRUE)}
is.exist <- "stringi" %in% rownames(installed.packages())
if(!is.exist){install.packages("stringi", dependencies=TRUE)}
is.exist <- "Synth" %in% rownames(installed.packages())
if(!is.exist){install.packages("Synth", dependencies=TRUE)}
is.exist <- "tidyr" %in% rownames(installed.packages())
if(!is.exist){install.packages("tidyr")}
is.exist <- "xts" %in% rownames(installed.packages())
if(!is.exist){install.packages("xts", dependencies=TRUE)}
is.exist <- "lubridate" %in% rownames(installed.packages())
if(!is.exist){install.packages("lubridate")}
rm(is.exist)

## read library
library(bit64)
library(car)
library(chron)
library(compute.es)
library(data.table)
library(dplyr)
library(dummies)
library(doParallel)
library(foreach)
library(GGally)
library(ggmcmc)
library(ggplot2)
library(lme4)
library(psych)
library(pwr)
library(ranger)
library(RCurl)
library(rstan)
library(stringi)
library(Synth)
library(tidyr)
library(xts)


## set memory limit
memory.limit(1024^4)

## set formatting
options(scipen = 5)

## function
lm.Beta2 = function(res) {
  ysd = sd(res$model[, 1]) # 結果変数のSD
  idv = res$model[, -1, drop = FALSE]
  N = ncol(idv)
  res.beta = sd = res$coefficients[-1]
  for (j in 1:N) {
    xxx = idv[, j]
    if (class(xxx) == "factor") {
      for (i in 2:nlevels(xxx)) {
        lab = paste(colnames(idv)[j], levels(xxx)[i], sep = "")
        dummy = as.integer(xxx) == i # 1/0 の変数???
        sd[lab] = sd(dummy)
      }
      
    } else {
      lab = colnames(idv)[j]
      sd[lab] = sd(xxx)
    }
  }
  res.beta * sd / ysd
}


Rawdata <- fread("Basic.txt")

Data <- Rawdata %>%
  dplyr::select(1, 2, 3, 8, 9, 11, 12, 17, 19, 20, 21, 22, 24, 25, 27, 28, 35, 36)

names(Data) = c("StoreCD",
                "Week",
                "JANCD",
                "Val",
                "Unit",
                "NumofSKUs",
                "SU",
                "MainBrandCD",
                "AreaCD",
                "ChannelCD",
                "LocationCD",
                "TradeAreaCD",
                "StoreSumVal",
                "StoreSumUnit",
                "StoreSumNumofSKUs",
                "StoreSumSU",
                "AvgCategorySales",
                "CategorySales")
Data$Week <- as.Date(Data$Week)

Data <- Data %>%
  dplyr::filter(LocationCD != c(5)) %>%
  dplyr::filter(LocationCD != c(6)) %>%
  dplyr::filter(TradeAreaCD != c(7)) %>%
  dplyr::filter(Val != "NA") %>%
  dplyr::filter(AreaCD != c(9)) %>% #AreaCD9
  dplyr::filter(Week < as.Date("2018/04/30")) #AreaCD9
  #dplyr::filter(Week >= as.Date("2017/04/03")) #AreaCD2
Data_Store <- as.data.frame(Data) %>%
  dplyr::group_by(StoreCD) %>%
  dplyr::summarise(
    AreaCD = mean(AreaCD),
    ChannelCD = mean(ChannelCD),
    LocationCD = mean(LocationCD),
    TradeAreaCD = mean(TradeAreaCD),
    AvgCategorySales = mean(AvgCategorySales)
  ) %>%
  dplyr::ungroup()

## create package list
PackageList <- fread("Package.csv")
PackageList$JANCD = as.character(PackageList$JANCD)
PackageList <- spread(PackageList, Size, Flg)
PackageList[is.na(PackageList)] <- 0

## calculate metrics about category
Data_Category_temp01 <- Data %>%
  dplyr::group_by(StoreCD, Week) %>%
  dplyr::summarise(
    Val_Category = sum(Val),
    Unit_Category = sum(Unit),
    SU_Category = sum(SU)
  ) %>%
  dplyr::mutate(Price_SU_Category = Val_Category / SU_Category)

## calculate metrics about Ariel
Data_Ariel_temp01 <- Data %>%
  dplyr::filter(MainBrandCD == "60120") %>%
  dplyr::inner_join(PackageList, by = "JANCD") %>%
  dplyr::group_by(StoreCD, Week) %>%
  dplyr::summarise(
    Val_Ariel = sum(Val),
    Unit_Ariel = sum(Unit),
    SU_Ariel = sum(SU),
    NumofSKUs_Ariel = sum(NumofSKUs),
    NumofSKUs_Ariel_AIPG_BTL = sum(AIPG_BTL),
    NumofSKUs_Ariel_AIPG_CTRG = sum(AIPG_CTRG),
    NumofSKUs_Ariel_AIPG_RFL = sum(AIPG_RFL),
    NumofSKUs_Ariel_ARIEL_SUD_RFL = sum(ARIEL_SUD_RFL),
    NumofSKUs_Ariel_ARIEL_SUD_TUB = sum(ARIEL_SUD_TUB),
    NumofSKUs_Ariel_ARIEL_SUD_CTRG = sum(ARIEL_SUD_CTRG),
    NumofSKUs_Ariel_ARIEL_SUD_SL = sum(ARIEL_SUD_SL),
    NumofSKUs_Ariel_AIPG_SL = sum(AIPG_SL),
    NumofSKUs_Ariel_ARIEL_SUD_SSL = sum(ARIEL_SUD_SSL),
    NumofSKUs_Ariel_AIPG_SSL = sum(AIPG_SSL),
    NumofSKUs_Ariel_OTHER = sum(OTHER)
  ) %>%
  dplyr::mutate(Price_SU_Ariel = Val_Ariel / SU_Ariel)

Data_Ariel_temp01 <- Data_Ariel_temp01 %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_AIPG_BTL = NumofSKUs_Ariel_AIPG_BTL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_AIPG_CTRG = NumofSKUs_Ariel_AIPG_CTRG / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_AIPG_RFL = NumofSKUs_Ariel_AIPG_RFL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_ARIEL_SUD_RFL = NumofSKUs_Ariel_ARIEL_SUD_RFL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_ARIEL_SUD_TUB = NumofSKUs_Ariel_ARIEL_SUD_TUB / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_ARIEL_SUD_CTRG = NumofSKUs_Ariel_ARIEL_SUD_CTRG / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_ARIEL_SUD_SL = NumofSKUs_Ariel_ARIEL_SUD_SL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_AIPG_SL = NumofSKUs_Ariel_AIPG_SL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_ARIEL_SUD_SSL = NumofSKUs_Ariel_ARIEL_SUD_SSL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_AIPG_SSL = NumofSKUs_Ariel_AIPG_SSL / NumofSKUs_Ariel) %>%
  dplyr::mutate(Prop_NumofSKUs_Ariel_OTHER = NumofSKUs_Ariel_OTHER / NumofSKUs_Ariel)

rm(PackageList)

## calculate about Attack
Data_Attack_temp01 <- Data %>%
  dplyr::filter(MainBrandCD == "75150") %>%
  dplyr::group_by(StoreCD, Week) %>%
  dplyr::summarise(
    Val_Attack = sum(Val),
    SU_Attack = sum(SU)
  ) %>%
  dplyr::mutate(Price_SU_Attack = Val_Attack / SU_Attack)

## unite
StoreCD_temp01 <- unique(Data_Ariel_temp01$StoreCD)
Week_temp01 <- unique(Data$Week)
Data_Ariel_temp00 <- expand.grid(StoreCD_temp01, Week_temp01)
colnames(Data_Ariel_temp00)[1] <- "StoreCD"
colnames(Data_Ariel_temp00)[2] <- "Week"
Data_Ariel_temp00$StoreCD <- as.integer(Data_Ariel_temp00$StoreCD)
Data_Ariel_temp00$Week <- as.Date(Data_Ariel_temp00$Week)

Week_temp02 <- as.data.frame(unique(Data_Ariel_temp00$Week)) %>%
  dplyr::distinct(Week) %>%
  mutate(Weeks = row_number())
colnames(Week_temp02)[1] <- "Week"

Data_Ariel_temp00 <- Data_Ariel_temp00 %>%
  dplyr::inner_join(Week_temp02, by = c("Week")) %>%
  dplyr::arrange(StoreCD, Week)

rm(StoreCD_temp01, Week_temp01, Week_temp02)

Data_Ariel_temp02 <- Data_Ariel_temp00 %>%
  dplyr::inner_join(Data_Store, by = c("StoreCD")) %>%
  dplyr::left_join(Data_Ariel_temp01, by = c("StoreCD", "Week")) %>%
  dplyr::left_join(Data_Category_temp01, by = c("StoreCD", "Week")) %>%
  dplyr::left_join(Data_Attack_temp01, by = c("StoreCD", "Week")) %>%
  dplyr::mutate(Share_Ariel = SU_Ariel / SU_Category) %>%
  dplyr::mutate(SU_exclAriel = SU_Category - SU_Ariel) %>%
  dplyr::mutate(Price_SU_exclAriel = (Val_Category - Val_Ariel) / (SU_Category - SU_Ariel)) %>%
  dplyr::arrange(StoreCD, Week)

rm(Data_Ariel_temp00)
rm(Data_Ariel_temp01)
rm(Data_Store, Data_Category_temp01, Data_Attack_temp01)

gc()
gc()

## make dummies
Data_Ariel_temp03 <- as.data.frame(Data_Ariel_temp02)
Data_Ariel_temp03$AreaCD <- as.character(Data_Ariel_temp03$AreaCD)
Data_Ariel_temp03$LocationCD <- as.character(Data_Ariel_temp03$LocationCD)
Data_Ariel_temp03$TradeAreaCD <- as.character(Data_Ariel_temp03$TradeAreaCD)

Data_Ariel_temp03 <- dummy.data.frame(Data_Ariel_temp03)
Data_Ariel_temp03 <- dummy.data.frame(Data_Ariel_temp03)

rm(Data_Ariel_temp02)

gc()
gc()

count_data <- Data_Ariel_temp03
count_data <- count_data[!duplicated(count_data$StoreCD),]

##omit copy stores
copy_stores <- read.csv("copy_stores.csv")
colnames(copy_stores) <- c("original", "copy")
count_data_2 <- count_data %>% anti_join(copy_stores, by = c("StoreCD" = "copy"))

##omit the stores whose SU_Ariel is 0 in pre-term or post-term
#omit pre 0
pre_zero <- Data_Ariel_temp03 %>%
  select(StoreCD, Weeks, SU_Ariel, AreaCD1, ChannelCD) %>%
  filter(Weeks <= 52)

pre_zero[is.na(pre_zero)] <- 0

pre_zero <- pre_zero %>%
  group_by(StoreCD) %>% 
  mutate(pre_SU_Ariel = sum(SU_Ariel))
pre_zero[pre_zero$pre_SU_Ariel == 0, "flg"] <- 1
pre_zero <- pre_zero %>% filter(flg == 1)
pre_zero <- pre_zero[!duplicated(pre_zero$StoreCD),]

#omit post 0
post_zero <- Data_Ariel_temp03 %>%
  select(StoreCD, Weeks, SU_Ariel, AreaCD1, ChannelCD) %>%
  filter(Weeks >= 53)

post_zero[is.na(post_zero)] <- 0

post_zero <- post_zero %>%
  group_by(StoreCD) %>% 
  mutate(post_SU_Ariel = sum(SU_Ariel))
post_zero[post_zero$post_SU_Ariel == 0, "flg"] <- 1
post_zero <- post_zero %>% filter(flg == 1)
post_zero <- post_zero[!duplicated(post_zero$StoreCD),]


#anti_join
count_data_3 <- count_data_2 %>% anti_join(pre_zero, c("StoreCD" = "StoreCD"))
count_data_4 <- count_data_3 %>% anti_join(post_zero, c("StoreCD" = "StoreCD"))
count_data_4 <- count_data_4 %>% select("StoreCD")
count_data_4$StoreCD <- as.character(count_data_4$StoreCD)


## preparation for synth
Data_Ariel_temp04 <- Data_Ariel_temp03
Data_Ariel_temp04$StoreCD <- as.character(Data_Ariel_temp04$StoreCD)
Data_Ariel_temp04[is.na(Data_Ariel_temp04)] <- 0

Data_Ariel_temp04 <-Data_Ariel_temp04 %>% inner_join(count_data_4, by = c("StoreCD" = "StoreCD"))
write.csv(Data_Ariel_temp04, "raw_data.csv")
length(unique(Data_Ariel_temp04$StoreCD))


Data_Ariel_StoreList_Test <- Data_Ariel_temp04 %>% 
dplyr::filter(AreaCD9 == 1) %>% #test area 9, 2
dplyr::select(StoreCD, ChannelCD) %>%
distinct(StoreCD, ChannelCD)

#rm(Data_Ariel_temp03)