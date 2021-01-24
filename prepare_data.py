import pandas as pd
import numpy as np
from data_reader import *
from os import walk
from sklearn.linear_model import LinearRegression
import shutil

nrows = 300

frames = read_tables(archive_path, nrows=nrows)

any_key = list(frames.keys())[0]

frame = frames[any_key]
datetimes = frame['datetime']
cities = []

for column in frame.columns[1:]:
    cities.append(column)


if True:
    # frame'y z medianami

    med_frames = {}
    for name in frames:
        med_frames[name] = frames[name].copy()

    # wypełnił brakujące dane przy użyciu interpolacji

    for name in frames:
        frame = frames[name]
        if name == 'weather_description':
            tmp_date = frame.iloc[0, 0]
            frame.iloc[0, :] = frame.iloc[1, :]
            frame.iloc[0, 0] = tmp_date
        else:
            frame = frame.interpolate(method='linear', axis=0, limit_direction='both')
            frame = round(frame)
        print(f'Filled {name}.csv')
        frames[name] = frame

    # zamień weather_desc na liczby

    wd = frames['weather_description']
    uniqes = wd.iloc[:, 2:].values.ravel()
    uniqes = pd.unique(uniqes)
    # print(uniqes)

    i = 0
    dict = {}
    for item in uniqes:
        dict[item] = i
        i = i + 1

    wd.replace(dict, inplace=True)

    frames['weather_description'] = wd




    # zapisz uzupełnione dane

    for name in frames:
        frame = frames[name]
        frame.to_csv(filled_path + name + '.csv', index=False)
        print(f'Written filled {name}.csv')

    # wypełnił brakujące dane przy użyciu median

    for name in med_frames:
        frame = med_frames[name]
        if name == 'weather_description':
            tmp_date = frame.iloc[0, 0]
            frame.iloc[0, :] = frame.iloc[1, :]
            frame.iloc[0, 0] = tmp_date
        else:
            frame.iloc[:, 1:] = frame.iloc[:, 1:].fillna(frame.iloc[:, 1:].median())
            frame = round(frame)
        print(f'Filled {name}.csv')
        med_frames[name] = frame


    # zapisz dane uzupełnione medianą

    for name in med_frames:
        frame = med_frames[name]
        frame.to_csv(filled_median_path + name + '.csv', index=False)
        print(f'Written filled {name}.csv')

    # stwórz data frame'y opisujące miasta
    def make_cities_frames(frames, path_to_save):
        _, _, filenames = next(walk(archive_path))
        namess = get_file_names_without_ext(filenames)

        col_names = ['datetime']
        col_names.extend(namess)

        for city in cities:
            data = [frame['datetime']]
            for par_name in file_names:
                data.append(frames[par_name][city])
            city_frame = pd.concat(data, axis=1, keys=col_names)
            print(f'Prepared frame for {city}')
            city_frame.to_csv(path_to_save + city + '.csv', index=False)
            print(f'Written frame for {city}')

    make_cities_frames(frames, cities_path)
    make_cities_frames(med_frames, cities_median_path)



    # better encoded cities

    wd = frames['weather_description']
    uniqes = wd.iloc[:, 2:].values.ravel()
    uniqes = pd.unique(uniqes)
    # print(uniqes)

    i = 0
    dict = {}
    for item in uniqes:
        dict[item] = i
        i = i + 1

    col_names = ['datetime']
    col_names.extend(file_names[:3])
    for uni in uniqes:
        col_names.append(uni)
    col_names.extend(file_names[4:])

    frames['humidity'].iloc[:, 1:] -= 50
    frames['pressure'].iloc[:, 1:] -= 1000
    frames['temperature'].iloc[:, 1:] -= 273
    frames['wind_direction'].iloc[:, 1:] -= 180



    for city in cities:
        data = [datetimes]
        for par_name in col_names[1:]:
            if par_name in file_names:
                data.append(frames[par_name][city])
            else:
                ohe = frames['weather_description'][city] == par_name
                data.append(ohe.astype(int))

        city_ohe_frame = pd.concat(data, axis = 1, keys=col_names)
        city_ohe_frame.to_csv(cities_ohe_path + city + '.csv', index=False)


def turn_into_points(directory_to_turn, directory_to_save_all, directory_to_save_separated=None, file_name_prefix='', geo=False):
    frames = read_tables(directory_to_turn, nrows=nrows)

    attr = pd.read_csv(archive_path + 'city_attributes.csv')

    first_midnight_index = 0
    any_frame = frames[list(frames.keys())[0]]

    frame_col_names = any_frame.columns[1:]
    if geo:
        col_names = ['latitude', 'longitude']
    else:
        col_names = []

    for i in range(72):
        for j in range(len(frame_col_names)):
            col_names.append(f'{i}_{frame_col_names[j]}')
    col_names.append('av_tmp_tomorrow')
    col_names.append('strong_winds_tomorrow')

    dates = any_frame['datetime']

    for i in range(len(dates)):
        if dates[i].hour == 0:
            first_midnight_index = i
            break

    nrow = any_frame.shape[0]
    ncol = any_frame.shape[1]

    all_data = []

    for city in cities:
        city_frame = frames[city]


        lat = attr.loc[attr['City'] == city]['Latitude'].iloc[0]
        lon = attr.loc[attr['City'] == city]['Longitude'].iloc[0]
        # print(f'{city}, {lat}-{lon}')

        city_data = []

        for i in range(first_midnight_index, nrow, 24):
            end_ind = i + 5 * 24 - 1
            if end_ind > nrow:
                break

            df = city_frame.iloc[i:i+72, 1:]
            t = []

            if geo:
                t = [lat, lon]

            for j in range(len(df.index)):
                t.extend(df.iloc[j, :].tolist())

            df2 = city_frame.iloc[i+96:i+120, 1:]
            means = df2.mean(axis=0)
            # print(means)
            av_tmp = means['temperature']
            # print(av_tmp)

            maxes = df2.max(axis=0)
            # print(maxes)
            max_wind = maxes['wind_speed']
            # print(max_wind)
            strong_winds = int(max_wind >= 8)

            t.extend([av_tmp, strong_winds])

            city_data.append(t[2:])
            all_data.append(t)

        print(f'Prepared points from {city}')

        if directory_to_save_separated is not None:
            df = pd.DataFrame.from_records(city_data, columns=col_names[2:])
            df.to_csv(directory_to_save_separated + file_name_prefix + f'{city}_' + 'points.csv', index=False)
            print(f'Written points from {city}')


    df = pd.DataFrame.from_records(all_data, columns=col_names)
    df.to_csv(directory_to_save_all + file_name_prefix + 'points.csv', index=False)
    print('Written points from cities')

def turn_into_aggregate_points(directory_to_turn, directory_to_save_all):
    frames = read_tables(directory_to_turn, nrows=nrows)

    for name in frames:
        frame = frames[name]
        frame = frame.loc[:, frame.columns != 'weather_description']
        frames[name] = frame

    first_midnight_index = 0
    any_frame = frames[list(frames.keys())[0]]

    frame_col_names = any_frame.columns[1:]
    col_names = []

    aggr_par_names = ['mean', 'std', 'slope']

    for i in range(len(frame_col_names)):
        for j in range(len(aggr_par_names)):
            col_names.append(f'{frame_col_names[i]}_{aggr_par_names[j]}')
    col_names.append('av_tmp_tomorrow')
    col_names.append('strong_winds_tomorrow')

    dates = any_frame['datetime']

    for i in range(len(dates)):
        if dates[i].hour == 0:
            first_midnight_index = i
            break

    nrow = any_frame.shape[0]
    ncol = any_frame.shape[1]

    all_data = []

    for city in cities:
        city_frame = frames[city]

        for i in range(first_midnight_index, nrow, 24):
            end_ind = i + 5 * 24 - 1
            if end_ind > nrow:
                break

            df = city_frame.iloc[i:i+72, 1:]
            t = []

            # for j in range(len(df.index)):
            #     t.extend(df.iloc[j, :].tolist())

            par_names = df.columns

            for name in par_names:
                col = df[name]
                avg = col.mean()

                std = col.std()

                x = np.array(list(range(72))).reshape((-1, 1))
                y = np.array(col)

                model = LinearRegression().fit(x, y)

                t.extend([avg, std, model.coef_[0]])

            df2 = city_frame.iloc[i+96:i+120, 1:]
            means = df2.mean(axis=0)
            # print(means)
            av_tmp = means['temperature']
            # print(av_tmp)

            maxes = df2.max(axis=0)
            # print(maxes)
            max_wind = maxes['wind_speed']
            # print(max_wind)
            strong_winds = int(max_wind >= 8)

            t.extend([av_tmp, strong_winds])
            all_data.append(t)

        print(f'Prepared points from {city}')

    df = pd.DataFrame.from_records(all_data, columns=col_names)
    df.to_csv(directory_to_save_all + 'aggregate_points.csv', index=False)
    print('Written aggregate points from cities')


turn_into_points(cities_path, processed_cities_normal_all, processed_cities_normal_sep, geo=True)
turn_into_points(cities_ohe_path, processed_cities_encoded_all)
turn_into_points(cities_median_path, processed_cities_median_all)
turn_into_aggregate_points(cities_path, aggregate_cities_path)
# shutil.rmtree(pre_processed_path)