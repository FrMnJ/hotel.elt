import pandas as pd
from command import Command
from extract_command import ExtractCommand

class CleanDataCommand(Command):
    def __init__(self, logger, datasets: dict):
        super().__init__(logger)
        self.datasets = datasets

    def execute(self):
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
        self.datasets['hotel_revenue_historical_full.xlsx'] = dataset2
        

                # Limpiando hotel_bookings_data.json
        self.logger.write_line("Limpiando hotel_bookings_data.json...")
        dataset3 = self.datasets['hotel_bookings_data.json']

        # Eliminar valores nulos en adr
        dataset3.dropna(subset=['adr'], inplace=True)

        # Eliminar 'INVALID_MONTH' en arrival_date_month
        dataset3 = dataset3[dataset3['arrival_date_month'] != 'INVALID_MONTH']

        # Eliminar valores 'XX' en assigned_room_type
        dataset3 = dataset3[dataset3['assigned_room_type'] != 'XX']

        # Eliminar valores 'UNKNOWN' en deposit_type
        dataset3 = dataset3[dataset3['deposit_type'] != 'UNKNOWN']

        # Eliminar filas donde company esté vacía o nula
        if 'company' in dataset3.columns:
            dataset3.dropna(subset=['company'], inplace=True)

        # Eliminar 'INVALID_COUNTRY' en country
        dataset3 = dataset3[dataset3['country'] != 'INVALID_COUNTRY']

        # Reemplazar lead_time > promedio por la media
        lead_time_mean = dataset3['lead_time'].mean()
        dataset3.loc[dataset3['lead_time'] > lead_time_mean, 'lead_time'] = lead_time_mean

        # Convertir reservation_status_date a datetime yyyy-mm-dd
        dataset3['reservation_status_date'] = pd.to_datetime(dataset3['reservation_status_date'], errors='coerce')
        dataset3.dropna(subset=['reservation_status_date'], inplace=True)

        # Reset index si se han eliminado muchas filas
        dataset3.reset_index(drop=True, inplace=True)

        self.datasets['hotel_bookings_data.json'] = dataset3

        
        return self.datasets

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
