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
        dataset1.dropna(inplace=True)

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

        self.logger.write_line("Limpiando hotel_revenue_historical.xlsx...")

        self.logger.write_line("Limpiando hotel_bookings_data.json...")
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