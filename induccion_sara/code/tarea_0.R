source('../auxiliar_functions.R')
source('_requirements.R')


# Get RIPS history --------------------------------------------------------
dict_path <- '../Data/RIPS_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/RIPS/'
(files <- list.files(folder, pattern = '((proc|cons).*201[0-2])|(urg|hosp)'))

# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('PERSONABASICAID', 'DATE_JUAN')

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
  
  service <- str_match(file, 'proc|cons|hosp|urg') %>% str_sub(end = 1L)
  
  df0 <- open_dataset(sprintf('%s/%s', folder, file)) %>% 
    # Select variables and rename.
    select(all_of(selected_columns_file)) %>%
    rename_at(vars(selected_columns_file), function(x) selected_columns1) %>%
    mutate(PERSONABASICAID = as.character(PERSONABASICAID)) %>% 
    filter(nchar(PERSONABASICAID) > 1, nchar(DATE_JUAN) >= 7) %>% 
    # Unify column types.
    mutate(
      across(all_of(selected_columns1[desired_classes == 'numeric']), as.numeric),
      across(all_of(selected_columns1[desired_classes == 'character']), as.character)
    ) %>% 
    # MODIFY: PROCESS THE DATA AS NEEDED. 
    mutate(
      SERVICE = str_sub(str_match(file, 'proc|cons|hosp|urg'), end = 1L),
      # DATE = str_sub(DATE_JUAN, 1, 7)
      YEAR = str_sub(DATE_JUAN, 1, 4) %>% as.integer,
      MONTH = str_sub(DATE_JUAN, 6, 7) %>% as.integer
    )
    
  if (service %in% c('h', 'u')) {
    df0 <- df0 %>% filter(between(YEAR, 2010L, 2012L))
  }
  df0 %>% 
    distinct(YEAR, MONTH, SERVICE, PERSONABASICAID) %>% 
    group_by(SERVICE, YEAR, MONTH) %>% summarise(n_personas = n()) %>% 
    collect

  df <- rbind(df, df0)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}
# MODIFY: PROCESS AND SAVE THE REQUIRED DATA.
df %>% write_parquet('../induccion_sara/tarea_0_RIPS.parquet')
sprintf('\n\t RIPS history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat

write_dta(df, '../induccion_sara/tarea_0_RIPS.dta')


# Get PILA history --------------------------------------------------------
dict_path <- '../Data/PILA_dictionary.xlsx'
dict <- read_excel(dict_path, sheet = 'colnames')
names(dict) <- sapply(str_split(names(dict), '\\.'),
                      function(x) paste(tail(x, 2), collapse = '.'))
# MODIFY: SELECT THE DESIRED FILES.
folder <- '../Data/PILA/'
(files <- list.files(folder, pattern = '201[0-2]'))
# MODIFY: SELECT THE DESIRED COLUMNS (USE THE UNINAME OF THE DICTIONARY).
selected_columns <- c('personabasicaid', 'fecha_cobertura')

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
    mutate(personabasicaid = as.character(personabasicaid)) %>% 
    filter(nchar(personabasicaid) > 1, nchar(fecha_cobertura) >= 7) %>% 
    # Unify column types.
    mutate(
      across(all_of(selected_columns1[desired_classes == 'numeric']), as.numeric),
      across(all_of(selected_columns1[desired_classes == 'character']), as.character)
    ) %>% 
    # MODIFY: PROCESS THE DATA AS NEEDED. 
    mutate(
      YEAR = str_sub(fecha_cobertura, 1, 4) %>% as.integer,
      MONTH = str_sub(fecha_cobertura, 6, 7) %>% as.integer
      ) %>%
    distinct(YEAR, MONTH, personabasicaid) %>% 
    group_by(YEAR, MONTH) %>% summarise(n_personas = n()) %>% 
    collect
  
  df <- rbind(df, df0)
  
  sprintf('\n\t Completed in %f secs.\n', get_values_tic_msg()) %>% cat
}
# MODIFY: PROCESS AND SAVE THE REQUIRED DATA.
df %>% write_parquet('../induccion_sara/tarea_0_PILA.parquet')
sprintf('\n\t PILA history retrieved in %f mins\n', get_values_tic_msg('min')) %>% cat

write_dta(df, '../induccion_sara/tarea_0_PILA.dta')

