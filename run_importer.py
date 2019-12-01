from importer.base import StockImporter

FILENAME_1 = 'csv_all_incoming.csv'
FILENAME_2 = 'csv_sample_1.csv'
FILENAME_3 = 'csv_sample_2.csv'
FILENAME_4 = 'csv_sample_3.csv'
FILENAME_5 = 'csv_sample_1_mod.csv'
FILENAME_6 = 'csv_all_incoming.zip'
FILENAME_7 = 'all_csv_merged.csv'

si = StockImporter()
si.process_all()
