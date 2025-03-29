# Resumen
Este repositorio contiene un proyecto ETL diseñado para extraer, limpiar y cargar datos en diferentes formatos según lo indique el usuario. Los formatos soportados incluyen CSV, JSON, EXCEL, y también permite la conexión y carga a una base de datos PostgreSQL.

# Objetivo de estudio
Predecir la probabilidad de que un cliente cancele una reservación con el objetivo de reducir la pérdida de ingresos de los hoteles. Al poder predecir podemos plantear estrategias para reducir el porcentaje de cancelación.

## Ejecución
```
pip install -r requirements.txt
python .\etl\main.py
```

## Metodología de recolección
Los datasets se buscaron en la plataforma Kaggle .

### Hotel bookings Dataset
Especificamente el conjunto principal de Hotel bookings: https://www.kaggle.com/datasets/mathsian/hotel-bookings Este dataset contiene información de las reservaciones de dos hoteles en Portugal. El hotel “Resort Hotel” en Algarve y el hotel “City Hotel” en Lisboa. En este dataset encontramos 119, 390 reservaciones con fechas desde 1 de Julio de 2015 y 31 de Agosto de 2017. Cada fila representa una reservación. El dataset presentado contiene el dataset original y una versión simplificada del mismo con solo 23 columnas. Y una versión aún más simplificada de 10 columnas como ayuda para el análisis. Debido a que los datos son reales, todos los elementos pertenecientes al hotel y los que identifican al cliente fueron eliminados. Los datos se encuentran en formato comma separated values (csv).

### Hotel Dataset
Se encontró también otro conjunto en Hotel Dataset: https://www.kaggle.com/datasets/mashkuratualhassan/hotel-dataset Presenta características similares al anterior, pero en este las reservaciones se encuentran dentro del periodo de 3 años entre 2018 y 2020. Los datos se encuentran en formato Excel (xlsx).

### Hotel bookings data
Se genero el tercer dataset de manera artificial en formato json. Los datos se encuentran dentro del periodo de 3 años entre 2022 y 2024.
