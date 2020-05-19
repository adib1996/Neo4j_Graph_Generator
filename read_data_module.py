import os
import pandas as pd


def remove_quotes(x):
    if x != "":
        return x[1:-1]
    else:
        return x


def read_data(input_data_path, triplets_file_names):
    for i in range(len(triplets_file_names)):
        if i == 0:
            data = pd.read_csv(os.path.join(input_data_path, triplets_file_names[i]), low_memory=False)
        else:
            data = pd.concat([data, pd.read_csv(os.path.join(input_data_path, triplets_file_names[i]), low_memory=False)])
    # Making the missing values to empty strings
    for col in list(data.columns):
        data.loc[data[col].isnull(), col] = ""
    # Checking whether Record Tag is present or not - if not create one
    if 'Record Tag' not in list(data.columns):
        data.loc[:, 'Record Tag'] = 'common'
    return data

