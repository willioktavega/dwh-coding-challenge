
import pandas as pd
import time
from datetime import datetime

pd.set_option('mode.chained_assignment', None)


def execute(key_column, raw_df, target_df):
    for index, row in raw_df.iterrows():
        tmp_df = target_df[(target_df[key_column] == row[key_column]) & (target_df['active_record'] == True)]
        index_tmp_df = target_df[(target_df[key_column] == row[key_column]) & (target_df['active_record'] == True)].index
        
        used_columns_df = row[['id', 'op', 'ts']]
        used_columns_df = used_columns_df.to_dict()
        try:
            if not pd.isnull(row['data']):
                data = pd.json_normalize(row['data']).to_dict(orient='records')[0]
                used_columns_df.update(data)
            if not pd.isnull(row['set']):
                data = pd.json_normalize(row['set']).to_dict(orient='records')[0]
                used_columns_df.update(data)
        except Exception as e:
            print('skip processing json data')

        if tmp_df.empty:
            fill_cols = [col for col in target_df.columns if col not in used_columns_df.keys()]
            time.sleep(1)
            for col in fill_cols:
                if col == 'active_record':
                    used_columns_df.update({'active_record': True})
                elif col == 'updated_ts':
                    used_columns_df.update({'updated_ts': datetime.now()})
                else:
                    # fill default value
                    dt = tmp_df[col].dtype
                    if dt == int or dt == float:
                        used_columns_df.update({col: 0})
                    else:
                        used_columns_df.update({col: ''})

            used_columns_df = pd.DataFrame.from_records([used_columns_df])
            target_df = pd.concat([target_df, used_columns_df]).reset_index(drop=True)
        else:
            fill_cols = [col for col in target_df.columns if col not in used_columns_df.keys()]
            time.sleep(1)
            for col in fill_cols:
                if col == 'active_record':
                    used_columns_df.update({'active_record': True})
                    tmp_df['active_record'] = False
                elif col == 'updated_ts':
                    used_columns_df.update({'updated_ts': datetime.now()})
                else:
                    used_columns_df.update(tmp_df[[col]].to_dict(orient='records')[0])

            used_columns_df = pd.DataFrame.from_records([used_columns_df])
            target_df.loc[index_tmp_df, :] = tmp_df[:]
            target_df = pd.concat([target_df, used_columns_df]).reset_index(drop=True)

        # print(target_df)
    return target_df