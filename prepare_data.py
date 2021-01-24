import pandas as pd
from data_reader import *
from os import walk

nrows = 500

frames = read_tables(archive_path, nrows=nrows)

any_key = list(frames.keys())[0]

frame = frames[any_key]
datetimes = frame['datetime']
cities = []

for column in frame.columns[2:]:
    cities.append(column)


if True:
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

    # zapisz uzupełnione dane

    for name in frames:
        frame = frames[name]
        frame.to_csv(filled_path + name + '.csv', index=False)
        print(f'Written filled {name}.csv')


    # stwórz data frame'y opisujące miasta
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
        city_frame.to_csv(cities_path + city + '.csv', index=False)
        print(f'Written frame for {city}')


    # ohe cities

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


def turn_into_points(directory_to_turn, directory_to_save_all, directory_to_save_separated=None, file_name_prefix=''):
    frames = read_tables(directory_to_turn, nrows=nrows)

    first_midnight_index = 0
    any_frame = frames[list(frames.keys())[0]]

    frame_col_names = any_frame.columns[1:]
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


        city_data = []

        for i in range(first_midnight_index, nrow, 24):
            end_ind = i + 5 * 24 - 1
            if end_ind > nrow:
                break

            df = city_frame.iloc[i:i+72, 1:]
            t = []

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

            city_data.append(t)
            all_data.append(t)

        print(f'Prepared points from {city}')

        if directory_to_save_separated is not None:
            df = pd.DataFrame.from_records(city_data, columns=col_names)
            df.to_csv(directory_to_save_separated + file_name_prefix + f'{city}_' + 'points.csv')
            print(f'Written points from {city}')


    df = pd.DataFrame.from_records(all_data, columns=col_names)
    df.to_csv(directory_to_save_all + file_name_prefix + 'points.csv', index=False)
    print('Written points from cities')

turn_into_points(cities_ohe_path, processed_path, processed_cities_normal_sep)