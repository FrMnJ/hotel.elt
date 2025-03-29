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

## Dataset 3: 

- **replace()**: Reemplaza los valores por otros de manera dinamica.

- **transform()** : Aplica una función a los valores de una columna.

- **astype()** : Castea los valores de una columna a otro tipo de dato dtype.
