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

- **loc()**: Acceda a un grupo de filas y columnas por etiqueta(s) o una matriz booleana.

- **std()** : Calcula la desviación estandar.

- **mean()** : Calcula el promedio.

- **dropna()** : Elimina las filas con NA.

- **drop_duplicates()**: Elimina las filas repetidas.

## Dataset 3: 

- **replace()**: Reemplaza los valores por otros de manera dinamica.