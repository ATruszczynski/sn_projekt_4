import pandas as pd
from data_reader import *
import os

frames = read_org_tables()

if not os.path.exists(pre_processed_path):
    os.makedirs(pre_processed_path)

if not os.path.exists(processed_path):
    os.makedirs(processed_path)

for name in frames:
    frame = frames[name]
    if name == 'weather_description':
        tmp_date = frame.iloc[0,0]
        frame.iloc[0,:] = frame.iloc[1,:]
        frame.iloc[0,0] = tmp_date
    else:
        frame = frame.interpolate(method='linear', axis=0, limit_direction='both')
        frame = round(frame)
    print(f'Preprocessed {name}.csv')
    frames[name] = frame

for name in frames:
    frame = frames[name]
    frame.to_csv(pre_processed_path + name + '.csv', index=False)
    print(f'Written preprocessed {name}.csv')




print(frames)

