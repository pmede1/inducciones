source('requirements.R')
library(dataRC); library(dataRC); library(arrow); library(dplyr); library(stringr)


# GET IDS FOR WOMEN BETWEEN 17 AND 24 YEARS -------------------------------

dict_path <- '../Data/RIPS_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/RIPS/'
(files <- list.files(folder, pattern = '(proc|cons|urg|hosp)'))

# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('PERSONABASICAID', 'SEXO', 'EDAD')

df <- NULL
tic()
for (file in files) {
  cat(paste('Began', file))
  tic()
 
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 

    unify_colnames(dict, file, selected_columns) %>% 
    unify_classes(dict, file, selected_columns) %>% 
    relocate_columns(selected_columns) %>% 
    
    mutate(PERSONABASICAID = as.character(PERSONABASICAID)) %>% 
    filter(nchar(PERSONABASICAID) > 1) %>% 
    # MODIFY: PROCESS THE DATA AS NEEDED.
    filter(SEXO == 'F', between(EDAD, 17, 25)) %>% 
    distinct(PERSONABASICAID) %>% collect
  
  df <- rbind(df, df0) %>% distinct(PERSONABASICAID)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}

df %>% distinct(PERSONABASICAID) %>%  write_parquet('../induccion_sara/tarea_1_RIPS.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat


# GET RIPS HISTORY --------------------------------------------------------

dict_path <- '../Data/RIPS_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/RIPS/'
(files <- list.files(folder))

# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('PERSONABASICAID', 'EDAD', 'DIAG_PRIN', 'DIAG_R1', 'COD_DIAG_R2', 'COD_DIAG_R3')

ids <- open_dataset('../induccion_sara/tarea_1_RIPS.parquet') %>% 
  select(PERSONABASICAID) %>% collect %>% unlist %>% unname

df <- NULL
tic()
for (file in files) {
  cat(paste('Began', file))
  tic()
  
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 
    unify_colnames(dict, file, selected_columns) %>% 
    unify_classes(dict, file, selected_columns) %>% 
    relocate_columns(selected_columns) %>% 
    
    filter(PERSONABASICAID %in% ids) %>% 
    mutate(PERSONABASICAID = as.character(PERSONABASICAID)) %>% 
    
    # MODIFY: PROCESS THE DATA AS NEEDED.
    distinct(PERSONABASICAID) %>% collect
  
  df <- rbind(df, df0) %>% distinct(PERSONABASICAID)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}

df %>% distinct(PERSONABASICAID) %>% write_parquet('../induccion_sara/tarea_1_hist_RIPS.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat


# GET PILA HISTORY --------------------------------------------------------
  
dict_path <- '../Data/PILA_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/PILA/'
(files <- list.files(folder))

# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('personabasicaid', 'fecha_cobertura', 'tipo_cotiz')

ids <- open_dataset('../induccion_sara/tarea_1_RIPS.parquet') %>% 
  select(PERSONABASICAID) %>% collect %>% unlist %>% unname

df <- NULL
tic()
for (file in files) {
  cat(paste('Began', file))
  tic()
  
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 
    unify_colnames(dict, file, selected_columns) %>% 
    unify_classes(dict, file, selected_columns) %>% 
    relocate_columns(selected_columns) %>% 
    
    filter(personabasicaid %in% ids) %>% 
    mutate(personabasicaid = as.character(personabasicaid)) %>% 
    
    # MODIFY: PROCESS THE DATA AS NEEDED.
    distinct(personabasicaid) %>% collect
  
  df <- rbind(df, df0) %>% distinct(personabasicaid)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}

df %>% distinct(personabasicaid) %>% write_parquet('../induccion_sara/tarea_1_hist_PILA.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat


