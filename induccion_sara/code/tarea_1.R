source('../auxiliar_functions.R')
source('_requirements.R')

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
  # Auxiliary variables to select proper names and get the desired types. 
  (df_selected <- filter(dict, uniname %in% selected_columns) %>% 
      select(uniname, uniclass, file) %>% drop_na(file) %>% 
      replace(is.na(.), ''))
  (selected_columns_file <- df_selected[[file]])
  (selected_columns1 <- df_selected$uniname)
  (desired_classes <- df_selected$uniclass)
  
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 
    # Select variables and rename.
    select(all_of(selected_columns_file)) %>%
    rename_at(vars(selected_columns_file), function(x) selected_columns1) %>%
    mutate(PERSONABASICAID = as.character(PERSONABASICAID)) %>% 
    filter(nchar(PERSONABASICAID) > 1) %>% 
    
    # Unify column types.
    mutate(
      across(all_of(selected_columns1[desired_classes == 'numeric']), as.numeric),
      across(all_of(selected_columns1[desired_classes == 'character']), as.character)
    ) %>% 
    # MODIFY: PROCESS THE DATA AS NEEDED.
    filter(SEXO == 'F', between(EDAD, 17, 25)) %>% 
    distinct(PERSONABASICAID) %>% collect
  
  df <- rbind(df, df0) %>% distinct(PERSONABASICAID)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}

df %>% distinct(PERSONABASICAID) %>%  write_parquet('../induccion_sara/tarea_1_RIPS.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat

## HISTORIAL RIPS
source('../auxiliar_functions.R')
source('_requirements.R')

dict_path <- '../Data/RIPS_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/RIPS/'
(files <- list.files(folder))

# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('PERSONABASICAID', 'EDAD', 'DIAG_PRIN', 'DIAG_R1', 'COD_DIAG_R2', 'COD_DIAG_R3')

ids <- df$PERSONABASICAID

df <- NULL
tic()
for (file in files) {
  cat(paste('Began', file))
  tic()
  # Auxiliary variables to select proper names and get the desired types. 
  (df_selected <- filter(dict, uniname %in% selected_columns) %>% 
      select(uniname, uniclass, file) %>% drop_na(file) %>% 
      replace(is.na(.), ''))
  (selected_columns_file <- df_selected[[file]])
  (selected_columns1 <- df_selected$uniname)
  (desired_classes <- df_selected$uniclass)
  
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 
    # Select variables and rename.
    select(all_of(selected_columns_file)) %>%
    rename_at(vars(selected_columns_file), function(x) selected_columns1) %>%
    filter(PERSONABASICAID = ids) %>% 
    mutate(PERSONABASICAID = as.character(PERSONABASICAID)) %>% 
    
    # Unify column types.
    mutate(
      across(all_of(selected_columns1[desired_classes == 'numeric']), as.numeric),
      across(all_of(selected_columns1[desired_classes == 'character']), as.character)
    ) %>% 
    # MODIFY: PROCESS THE DATA AS NEEDED.
    distinct(PERSONABASICAID) %>% collect
  
  df <- rbind(df, df0) %>% distinct(PERSONABASICAID)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}

df %>% distinct(PERSONABASICAID) %>%  write_parquet('../induccion_sara/tarea_1_hist_RIPS.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat

  


