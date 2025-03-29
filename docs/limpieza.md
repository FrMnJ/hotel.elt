# Limpieza

## Dataset 1: hotel_booking_demand.csv

- **where()**: Filtra un dataframe según una condición booleana. Las filas que no cumplan la condición son reemplazadas por NaN (si no se usa inplace=True). Se utiliza para excluir o filtrar datos específicos, como eliminar duplicados o valores fuera de rango. Se usó para eliminar las filas duplicadas
`dataset1 = dataset1.where(~dataset1.duplicated()) `

- **query()**: Permite filtrar un dataframe usando una expresión en forma de cadena. Filtra las filas que cumplen con las condiciones dadas. Se utiliza para eliminar las filas con 0 o más de 10 adultos
`dataset1 = dataset1.query("0 < adults <= 10")`

- **isin()**: Comprueba si los valores de una columna están presentes en una lista o conjunto de valores dados. Eliminamos los que tengan Undefined. 
`dataset1 = dataset1[~dataset1['meal'].isin(['Undefined'])]`
`dataset1 = dataset1[~dataset1['market_segment'].isin(['Undefined'])]`
`dataset1 = dataset1[~dataset1['distribution_channel'].isin(['Undefined'])]`

- **apply()**: Hace una función a lo largo de los elementos de una columna o fila. Se rellena con el valor de la moda.
`dataset1['agent'] = dataset1['agent'].apply(lambda x: mode_value if pd.isna(x) else x)`

- **pop()**: Elimina una columna. 
  `dataset1.pop('company')`


## Dataset 2: hotel_revenue_historical_full.xlsx

- **loc()**: Accede a un grupo de filas y columnas por etiqueta(s) o mediante una matriz booleana. Por ejemplo, se usa para ajustar valores en la columna "lead_time":
  `dataset2.loc[dataset2['lead_time'] > lead_time_mean, 'lead_time'] = lead_time_mean`

- **mean()**: Calcula el promedio de los valores de una columna. Por ejemplo, se utiliza para obtener el promedio de "lead_time":
  `lead_time_mean = dataset2['lead_time'].mean()`

- **drop_duplicates()**: Elimina filas duplicadas del dataframe.
  `dataset2 = dataset2.drop_duplicates()`

- **dropna()**: Elimina filas que contengan valores NA en las columnas especificadas. Se aplica, por ejemplo, para eliminar filas nulas en "children" o "country" o "agent":
  `dataset2 = dataset2.dropna(subset=['children'])`

- **drop()**: Se utiliza para eliminar filas o columnas. En este caso, se elimina:
  - Filas donde "babies" es mayor a 2:
    `dataset2 = dataset2.drop(dataset2[dataset2['babies'] > 2].index)`
  - La columna "company":
    `dataset2 = dataset2.drop(columns=['company'])`

- **Filtrado mediante condiciones booleanas**: Se filtran filas usando condiciones directamente en el dataframe. Por ejemplo:
  - Eliminar filas con 0 en "adults":
    `dataset2 = dataset2[dataset2['adults'] != 0]`
  - Filtrar filas donde "meal", "market_segment" y "distribution_channel" sean diferentes de "Undefined":
    `dataset2 = dataset2[dataset2['meal'] != 'Undefined']`
    `dataset2 = dataset2[dataset2['market_segment'] != 'Undefined']`
    `dataset2 = dataset2[dataset2['distribution_channel'] != 'Undefined']`


## Dataset 3: hotel_bookings_data.json

- **notna()**: Filtra filas para trabajar solo con registros completos.  
  Se utiliza para asegurar que las columnas "adr", "arrival_date_month" y "reservation_status_date" no tengan valores nulos.  
  `dataset3 = dataset3[dataset3['adr'].notna()]`  
  `dataset3 = dataset3[dataset3['arrival_date_month'].notna()]`  
  `dataset3 = dataset3[dataset3['reservation_status_date'].notna()]`

- **replace()**: Reemplaza valores específicos en una columna por otros.  
  Se usa para cambiar 'INVALID_MONTH' a pd.NA en "arrival_date_month" y 'UNKNOWN' a 'Otro' en "deposit_type".  
  `dataset3['arrival_date_month'] = dataset3['arrival_date_month'].replace('INVALID_MONTH', pd.NA)`  
  `dataset3['deposit_type'] = dataset3['deposit_type'].replace('UNKNOWN', 'Otro')`

- **str.startswith()**: Verifica si los valores en una columna comienzan con una cadena dada.  
  Se utiliza para excluir filas cuya "assigned_room_type" comienza con 'X'.  
  `dataset3 = dataset3[~dataset3['assigned_room_type'].str.startswith('X', na=False)]`

- **drop()**: Elimina columnas o filas del dataframe.  
  Se usa con el parámetro "columns" para eliminar la columna "company".  
  `dataset3.drop(columns=['company'], inplace=True)`

- **str.fullmatch()**: Comprueba coincidencias exactas de una cadena en cada elemento de una columna.  
  Se utiliza para descartar filas donde la columna "country" es exactamente "INVALID_COUNTRY".  
  `dataset3 = dataset3[~dataset3['country'].str.fullmatch('INVALID_COUNTRY', na=False)]`

- **median()**: Calcula la mediana de los valores de una columna.  
  Se usa para obtener la mediana de "lead_time", que luego se utiliza para ajustar los valores excesivos.  
  `median_lt = dataset3['lead_time'].median()`

- **transform()**: Aplica una función a cada elemento de la serie.  
  En este caso, se utiliza para reemplazar valores de "lead_time" que superen la mediana por dicha mediana.  
  `dataset3['lead_time'] = dataset3['lead_time'].transform(lambda x: x if x <= median_lt else median_lt)`

- **astype()**: Convierte los valores de una columna a un tipo de dato específico.  
  Se utiliza para convertir "reservation_status_date" al tipo datetime.  
  `dataset3['reservation_status_date'] = dataset3['reservation_status_date'].astype('datetime64[ns]', errors='ignore')`

- **sort_values()**: Ordena las filas del dataframe según los valores de una o más columnas.  
  Se usa para ordenar "reservation_status_date" de forma cronológica.  
  `dataset3 = dataset3.sort_values(by='reservation_status_date')`

- **fillna()**: Rellena los valores nulos en una columna con un valor dado.  
  Se aplica en "required_car_parking_spaces" para reemplazar NaN por 0.0.  
  `dataset3['required_car_parking_spaces'].fillna(0.0, inplace=True)`
