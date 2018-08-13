library(gsynth)

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
library(rgenoud)

## function
lm.Beta2 = function(res) {
  ysd = sd(res$model[, 1])
  idv = res$model[, -1, drop = FALSE]
  N = ncol(idv)
  res.beta = sd = res$coefficients[-1]
  for (j in 1:N) {
    xxx = idv[, j]
    if (class(xxx) == "factor") {
      for (i in 2:nlevels(xxx)) {
        lab = paste(colnames(idv)[j], levels(xxx)[i], sep = "")
        dummy = as.integer(xxx) == i
        sd[lab] = sd(dummy)
      }
      
    } else {
      lab = colnames(idv)[j]
      sd[lab] = sd(xxx)
    }
  }
  res.beta * sd / ysd
}


## change directory
#setwd("~/P&G")

channel_list <- c("GMS", "SML", "SMS", "miniSM", "CVS", "HCDS", "DrugL", "DrugS")

##read data file(omit the same record stores and 0 stores)
rawData <- fread("raw_data.csv")
rawData <- rawData %>% select(-1)

##loop gsynth by channel
i <- 8
#for(i in 6:8){
  ##get GMS channel (channelCD==1)
  sub_data <- rawData %>%
    dplyr::filter(ChannelCD == 8)
  
  sub_data$D <- as.numeric((sub_data$Weeks>52)&(sub_data$AreaCD2==1)) #test area 9, 2
  
  #step-wise
  #sub_data_52 <- sub_data %>% filter(Weeks <= 52)
  lm_sub_data <- lm(SU_Ariel ~ NumofSKUs_Ariel + 
                      NumofSKUs_Ariel_AIPG_BTL + 
                      NumofSKUs_Ariel_AIPG_CTRG + 
                      NumofSKUs_Ariel_AIPG_RFL + 
                      NumofSKUs_Ariel_ARIEL_SUD_RFL + 
                      NumofSKUs_Ariel_ARIEL_SUD_TUB + 
                      NumofSKUs_Ariel_ARIEL_SUD_CTRG + 
                      NumofSKUs_Ariel_ARIEL_SUD_SL + 
                      NumofSKUs_Ariel_AIPG_SL + 
                      NumofSKUs_Ariel_ARIEL_SUD_SSL + 
                      NumofSKUs_Ariel_AIPG_SSL + 
                      Prop_NumofSKUs_Ariel_AIPG_BTL + 
                      Prop_NumofSKUs_Ariel_AIPG_CTRG + 
                      Prop_NumofSKUs_Ariel_AIPG_RFL + 
                      Prop_NumofSKUs_Ariel_ARIEL_SUD_RFL +
                      Prop_NumofSKUs_Ariel_ARIEL_SUD_TUB + 
                      Prop_NumofSKUs_Ariel_ARIEL_SUD_CTRG + 
                      Prop_NumofSKUs_Ariel_ARIEL_SUD_SL + 
                      Prop_NumofSKUs_Ariel_AIPG_SL + 
                      Prop_NumofSKUs_Ariel_ARIEL_SUD_SSL + 
                      Prop_NumofSKUs_Ariel_AIPG_SSL +
                      Price_SU_Ariel+
                      Price_SU_Category+
                      Val_Attack+
                      SU_Attack+
                      Price_SU_Attack+
                      SU_exclAriel+
                      Price_SU_exclAriel, data = sub_data)
  step_lm_sub_data <- step(lm_sub_data)
  # •Ï”“¾‚é‚½‚ß‚Élm.Beta2‚É“ü‚ê‚é
  step_standardized <- lm.Beta2(step_lm_sub_data) %>% as.data.frame()
  #predictor‚É‚·‚é
  candidates_2 <- c(row.names(step_standardized))

  gsynth_formula <- as.formula(paste("SU_Ariel ~ D+", paste(candidates_2, collapse = "+")))
    
  ##Run gsynth for all test stores
  out <- gsynth(gsynth_formula, data = sub_data,
                index = c("StoreCD","Weeks"), force = "two-way",
                CV = TRUE, r = c(0, 5), se = TRUE,
                inference = "parametric", nboots = 1000,
                parallel = TRUE, cores = 8)
  
  
  ##weights
  df_w <- out$wgt.implied

  write.csv(df_w, paste0("df_w_output", channel_list[i], ".csv"))
  

  #beta
  df_b <- out$beta
  write.csv(df_b, paste0("df_b_output", channel_list[i],".csv"))
  
  
  #output
  headers<-c("actual","predict")
  df <- as.data.frame(matrix(ncol=2,nrow=60))
  names(df)<-headers
  
  
  df$actual <- out[["Y.tr"]]
  df$predict<- out[["Y.ct"]]
  
  write.csv(df, paste0("df_output", channel_list[i], ".csv"))

