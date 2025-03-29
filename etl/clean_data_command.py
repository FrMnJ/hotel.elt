import pandas as pd
from command import Command

class CleanDataCommand(Command):
    def __init__(self, logger):
        super().__init__(logger)

    def execute(self, datasets):
        self.datasets = datasets
        self.logger.write_line("Limpiando hotel_booking_demand.csv...")
        dataset1 = self.datasets['hotel_booking_demand.csv'] 
        
        # Eliminar los duplicados
        dataset1.drop_duplicates(inplace=True)

        # Eliminar los valores nulos
        #dataset1.dropna(inplace=True)

        # Mayores al promedio reemplazados por la media
        dataset1.loc[dataset1['lead_time'] > dataset1['lead_time'].mean(), 'lead_time'] = int(dataset1['lead_time'].mean())

        # Eliminar filas con 0 adultos y > 10
        dataset1 = dataset1[(dataset1['adults'] > 0) | (dataset1['adults'] <= 10)]

        # Eliminar files con más de 2 babies
        dataset1.drop(dataset1[dataset1['babies'] > 2].index, inplace=True)

        # Eliminar Undefined en la columna meal
        dataset1 = dataset1[dataset1['meal'] != 'Undefined']

        # Eliminar Undefined en la columna market_segment
        dataset1 = dataset1[dataset1['market_segment'] != 'Undefined']

        # Eliminar Undefined en la columna distribution_channel
        dataset1 = dataset1[dataset1['distribution_channel'] != 'Undefined']

        # Eliminar filas mayores de 6 previous_cancellations
        dataset1.drop(dataset1[dataset1['previous_cancellations'] > 6].index, inplace=True)

        # Eliminar filas mayores de 4 previous_bookings_not_canceled
        dataset1.drop(dataset1[dataset1['previous_bookings_not_canceled'] > 4].index, inplace=True)

        # Eliminar files con booking_changes > 5
        dataset1.drop(dataset1[dataset1['booking_changes'] > 5].index, inplace=True)

        #  Rellenar agent con la moda
        dataset1['agent'].fillna(dataset1['agent'].mode()[0], inplace=True)

        # Eliminar filas nulas en 'country'
        dataset1 = dataset1.dropna(subset=['country'])

        # Borrar la columna company
        dataset1.drop(columns=['company'], inplace=True)

        # Obtener la media de la frecuencia de la columna days_in_waiting_list y reemplazar los que tenga frecuencia menor a la media
        value_counts_mean = dataset1['days_in_waiting_list'].value_counts().mean()
        condition = dataset1['days_in_waiting_list'].map(dataset1['days_in_waiting_list'].value_counts()) < value_counts_mean
        dataset1.loc[condition, 'days_in_waiting_list'] = int(dataset1['days_in_waiting_list'].mean())

        # Reemplazar por promedio si es menor igual a cero o esta a +-dos desviaciones en adr
        std_dev_adr = dataset1['adr'].std()
        mean_adr = dataset1['adr'].mean()
        lower_bound = mean_adr - 2 * std_dev_adr
        upper_bound = mean_adr + 2 * std_dev_adr
        dataset1.loc[(dataset1['adr'] <= 0) | (dataset1['adr'] < lower_bound) | (dataset1['adr'] > upper_bound), 'adr'] = mean_adr
        
        # reservation_status_date Obtener la media de la frecuencia y borrar los menores a esta.
        counts = dataset1['reservation_status_date'].value_counts()
        mean_frequency = counts.mean()
        valid_values = counts[counts >= mean_frequency].index
        dataset1 = dataset1[dataset1['reservation_status_date'].isin(valid_values)]

        dataset1.drop_duplicates(inplace=True)
        # Reemplazar null con 0.0 in required_car_parking_spaces
        dataset1.loc[dataset1['required_car_parking_spaces'].isnull(), 'required_car_parking_spaces'] = 0.0

        dataset1['total_guests'] = dataset1['adults'] + dataset1['children'] + dataset1['babies']
        dataset1['is_long_stay'] = dataset1['stays_in_weekend_nights'] + dataset1['stays_in_week_nights'] > 7
        dataset1['total_nights'] = dataset1['stays_in_weekend_nights'] + dataset1['stays_in_week_nights']
        self.datasets['hotel_booking_demand.csv'] = dataset1

        ## Añadir el dataset limpio devuelta al diccionario
        self.datasets['hotel_booking_demand.csv'] = dataset1


        # Limpiando hotel_revenue_historical_full.xlsx
        self.logger.write_line("Limpiando hotel_revenue_historical_full.xlsx...")
        dataset2 = self.datasets['hotel_revenue_historical_full.xlsx']
        
        # Eliminar duplicados
        dataset2 = dataset2.drop_duplicates()
        
        # Ajustar 'lead_time'
        lead_time_mean = dataset2['lead_time'].mean()
        dataset2.loc[dataset2['lead_time'] > lead_time_mean, 'lead_time'] = lead_time_mean
        
        # Eliminar filas con 0 en 'adults'
        dataset2 = dataset2[dataset2['adults'] != 0]
        
        # Eliminar filas nulas en 'children'
        dataset2 = dataset2.dropna(subset=['children'])
        
        # Eliminar filas con más de 2 babies
        dataset2 = dataset2.drop(dataset2[dataset2['babies'] > 2].index)
        
        # Filtrar 'meal' diferente de 'Undefined'
        dataset2 = dataset2[dataset2['meal'] != 'Undefined']
        
        # Eliminar filas nulas en 'country'
        dataset2 = dataset2.dropna(subset=['country'])
        
        # Filtrar market_segment diferente de 'Undefined'
        dataset2 = dataset2[dataset2['market_segment'] != 'Undefined']
        
        # Filtrar distribution_channel diferente de 'Undefined'
        dataset2 = dataset2[dataset2['distribution_channel'] != 'Undefined']
        
        # Eliminar filas nulas en 'agent'
        dataset2 = dataset2.dropna(subset=['agent'])
        
        # Eliminar la columna 'company'
        dataset2 = dataset2.drop(columns=['company'])

        dataset2 = dataset2.drop_duplicates()
        # Reemplazar null con 0.0 in required_car_parking_spaces
        dataset1.loc[dataset1['required_car_parking_spaces'].isnull(), 'required_car_parking_spaces'] = 0.0

        dataset2['total_guests'] = dataset2['adults'] + dataset2['children'] + dataset2['babies']
        dataset2['is_long_stay'] = dataset2['stays_in_weekend_nights'] + dataset2['stays_in_week_nights'] > 7
        dataset2['total_nights'] = dataset2['stays_in_weekend_nights'] + dataset2['stays_in_week_nights']
        self.datasets['hotel_revenue_historical_full.xlsx'] = dataset2

        self.datasets['hotel_revenue_historical_full.xlsx'] = dataset2
        
        # Limpiando hotel_bookings_data.json
        self.logger.write_line("Limpiando hotel_bookings_data.json...")
        dataset3 = self.datasets['hotel_bookings_data.json']

        # Eliminamos todas las filas que tenían valores vacíos (nulos) en la columna adr usando .notna().
        # Esto nos asegura trabajar solo con datos completos en esa columna, ya que adr representa el precio
        # promedio por noche y es un valor clave para cualquier análisis relacionado con ingresos, tarifas o rendimiento financiero.
        # Como no tiene sentido estimar este valor sin una base sólida, preferimos eliminar esas filas para mantener la calidad del análisis.
        dataset3 = dataset3[dataset3['adr'].notna()]

        # Cambiamos el texto 'INVALID_MONTH' por un valor nulo, y después quitamos esas filas.
        # Así nos aseguramos de quedarnos solo con meses válidos.
        dataset3['arrival_date_month'] = dataset3['arrival_date_month'].replace('INVALID_MONTH', pd.NA)
        dataset3 = dataset3[dataset3['arrival_date_month'].notna()]

        # Filtramos todas las filas donde el tipo de habitación empieza con 'X' (que significa que está mal).
        # Esto lo hicimos con una función que detecta si una cadena empieza con cierta letra.
        dataset3 = dataset3[~dataset3['assigned_room_type'].str.startswith('X', na=False)]

        # Reemplazamos el valor 'UNKNOWN' en la columna 'deposit_type' por algo más entendible: 'Otro'.
        # Así evitamos quedarnos con categorías sin sentido.
        dataset3['deposit_type'] = dataset3['deposit_type'].replace('UNKNOWN', 'Otro')


        # Eliminamos la columna 'company'
        dataset3.drop(columns=['company'], inplace=True)

        # Quitamos las filas donde el país fuera 'INVALID_COUNTRY', usando una función que revisa coincidencias exactas de texto.
        # Así nos aseguramos de trabajar solo con países válidos.
        dataset3 = dataset3[~dataset3['country'].str.fullmatch('INVALID_COUNTRY', na=False)]

        # En la columna 'lead_time', bajamos los valores que eran demasiado altos al nivel de la mediana.
        # Lo hicimos con una función que revisa uno por uno y los ajusta si se pasan de la raya.        
        median_lt = dataset3['lead_time'].median()
        dataset3['lead_time'] = dataset3['lead_time'].transform(lambda x: x if x <= median_lt else median_lt)

        # Cambiamos las fechas a formato de fecha real, y luego ordenamos el dataset de más antiguo a más reciente.
        # Así es más fácil analizar por tiempo y asegurarse que todo esté en orden.
        dataset3['reservation_status_date'] = dataset3['reservation_status_date'].astype('datetime64[ns]', errors='ignore')
        dataset3 = dataset3[dataset3['reservation_status_date'].notna()]
        dataset3 = dataset3.sort_values(by='reservation_status_date')

        # Reemplazar null con 0.0 in required_car_parking_spaces
        dataset3['required_car_parking_spaces'].fillna(0.0, inplace=True)

        dataset3['total_guests'] = dataset3['adults'] + dataset3['children'] + dataset3['babies']
        dataset3['is_long_stay'] = dataset3['stays_in_weekend_nights'] + dataset3['stays_in_week_nights'] > 7
        dataset3['total_nights'] = dataset3['stays_in_weekend_nights'] + dataset3['stays_in_week_nights']
        self.datasets['hotel_bookings_data.json'] = dataset3

        self.datasets['hotel_bookings_data.json'] = dataset3

        return pd.concat(self.datasets.values(), ignore_index=True)

    def undo(self):
        pass


if __name__ == '__main__':
    from debug_logger import DebugLogger
    from extract_all_datasets import ExtractAllDatasetsCommand
    logger = DebugLogger()
    datasets = ExtractAllDatasetsCommand(logger).execute()
    cleanCommand = CleanDataCommand(logger, datasets)
    datasets = cleanCommand.execute()
    datasets['hotel_booking_demand.csv'].to_csv('hotel_booking_demand_cleaned.csv', index=False)
    datasets['hotel_revenue_historical_full.xlsx'].to_excel('hotel_revenue_historical_full_cleaned.xlsx', index=False)
    datasets['hotel_bookings_data.json'].to_json('hotel_bookings_data_cleaned.json', index=False)
