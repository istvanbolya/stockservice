from service.service import StockService

FILENAME_1 = 'csv_all_incoming.csv'
FILENAME_2 = 'all_csv_merged.csv'

service = StockService()
service.get_events()
