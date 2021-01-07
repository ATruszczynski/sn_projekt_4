import pandas as pd
import warnings

file_names = ['humidity', 'pressure', 'temperature', 'weather_description', 'wind_direction', 'wind_speed']

archive_path_alek = f'C:\\Users\\aleks\\Desktop\\archive\\'

archive_path = archive_path_alek

pre_processed_path = archive_path + 'pre_processed\\'
processed_path = archive_path + 'processed\\'

def read_tables(path, nrows=None):
    result_dict = {}

    for file in file_names:
        result_dict[file] = pd.read_csv(path + file + '.csv', nrows=nrows)
        if not pd.DataFrame.all(result_dict[file].notnull(), axis=None):
            warnings.warn(f'There are nulls in {file}.csv')

    return result_dict


def read_org_tables(nrows=None):
    return read_tables(archive_path, nrows=nrows)

def read_new_tables(nrows=None):
    return read_tables(pre_processed_path, nrows=nrows)

