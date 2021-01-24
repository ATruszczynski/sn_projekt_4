import pandas as pd
import warnings
import os
from os import walk

file_names = ['humidity', 'pressure', 'temperature', 'weather_description', 'wind_direction', 'wind_speed']

archive_path_alek = f'C:\\Users\\aleks\\Desktop\\archive\\'

archive_path = archive_path_alek

pre_processed_path = archive_path + 'pre_processed\\'
processed_path = archive_path + 'processed\\'
processed_cities_normal_all = processed_path + 'cities_all\\'
processed_cities_normal_sep = processed_path + 'cities_sep\\'
filled_path = pre_processed_path + 'filled\\'
cities_ohe_path = pre_processed_path + 'cities\\ohe\\'
cities_path = pre_processed_path + 'cities\\normal\\'

paths_to_ensure = [pre_processed_path, processed_path, processed_cities_normal_all, processed_cities_normal_sep, filled_path, cities_ohe_path, cities_path]

for path in paths_to_ensure:
    if not os.path.exists(path):
        os.makedirs(path)

def get_file_names_without_ext(filenames):
    result = []
    for filename in filenames:
        if filename != 'city_attributes.csv':
            result.append(os.path.splitext(filename)[0])
    return result

def read_tables(path, nrows=None):
    result_dict = {}

    _, _, filenames = next(walk(path))
    file_names = get_file_names_without_ext(filenames)

    for file in file_names:
        result_dict[file] = pd.read_csv(path + file + '.csv', nrows=nrows)
        result_dict[file]['datetime'] = pd.to_datetime(result_dict[file]['datetime'])
        # if not pd.DataFrame.all(result_dict[file].notnull(), axis=None):
        #     warnings.warn(f'There are nulls in {file}.csv')

    return result_dict


# def read_org_tables(nrows=None):
#     return read_tables(archive_path, nrows=nrows)
#
# def read_new_tables(nrows=None):
#     return read_tables(filled_path, nrows=nrows)

