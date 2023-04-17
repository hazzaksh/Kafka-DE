import pandas as pd
import json
import io
count = 1
def fetch_data(data, df_c, new_df):
    global count
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'ColData':
                row_dict = {}
                id_value = next((x for x in value if 'id' in x), None)

                if id_value:
                    row_dict['id'] = str(id_value['id'])
               
                row_dict['sid'] = str(count)
                count+=1
                row_dict[str(df_c['Column'][0]['ColTitle'])] = str(value[0]['value'])
                row_dict[str(df_c['Column'][1]['ColTitle'])] = str(value[1]['value'])
                new_df = pd.concat([new_df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)
            else:
                new_df = fetch_data(value, df_c, new_df)
            
    elif isinstance(data, list):
        for item in data:
            new_df = fetch_data(item, df_c, new_df)

    return new_df

def start():
   with open('balancesheet.json') as file:
      data = json.load(file)
   df_c = pd.DataFrame(data['Columns'])
   df = fetch_data(data, df_c, pd.DataFrame())
   csv_data = df.to_csv(index=False,header=True,path_or_buf=None)
   print(csv_data)
   return csv_data

# Written by me
def start_my(json_data):
    # data = json.load(json_data)
    df_c = pd.DataFrame(json_data['Columns'])
    df = fetch_data(json_data, df_c, pd.DataFrame())
    df_c = pd.DataFrame(json_data['Columns'])
    df = fetch_data(json_data, df_c, pd.DataFrame())
    csv_data = df.to_csv(index=False,header=True,path_or_buf=None)
    return csv_data
   

# start()
