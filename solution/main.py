import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import os

import processing

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

if not os.environ.get('WORKDIR'):
    raise Exception("'WORKDIR' env var can't be empty")

workdir = os.environ.get('WORKDIR')
datadir = os.path.join(workdir, 'data')

cards_df = pd.DataFrame()
saving_accounts_df = pd.DataFrame()
accounts_df = pd.DataFrame()

data_path = {}
if os.path.exists(datadir):
    for each in os.scandir(datadir):
        if os.path.isdir(each.path):
            dir_name = os.path.split(each.path)[-1]
            data_path[dir_name] = each.path

for dir_name in data_path.keys():
    log_success = True
    paths = Path(data_path.get(dir_name)).glob("*.json")
    if dir_name == 'cards':
        cards_df = pd.DataFrame([pd.read_json(p, typ="series") for p in paths]).sort_values(by='ts').reset_index(drop=True)
    elif dir_name == 'savings_accounts':
        saving_accounts_df = pd.DataFrame([pd.read_json(p, typ="series") for p in paths]).sort_values(by='ts').reset_index(drop=True)
    elif dir_name == 'accounts':
        accounts_df = pd.DataFrame([pd.read_json(p, typ="series") for p in paths]).sort_values(by='ts').reset_index(drop=True)
    else:
        log_success = False
        print('Unknown directories: {}'.format(data_path.get(dir_name)))

    if log_success:
        print('Data Loaded: {}'.format(data_path.get(dir_name)))


cards_denormalize_df = pd.DataFrame({
    'id': pd.Series(dtype='string'),
    'op': pd.Series(dtype='string'),
    'ts': pd.Series(dtype='int64'),
    'card_id': pd.Series(dtype='string'),
    'card_number': pd.Series(dtype='string'),
    'credit_used': pd.Series(dtype='int64'),
    'monthly_limit': pd.Series(dtype='int64'),
    'status': pd.Series(dtype='string'),
    'active_record': pd.Series(dtype='bool'),
    'updated_ts': pd.Series(dtype='datetime64[ns]'),
})
accounts_denormalize_df = pd.DataFrame({
    'id': pd.Series(dtype='string'),
    'op': pd.Series(dtype='string'),
    'ts': pd.Series(dtype='int64'),
    'account_id': pd.Series(dtype='string'),
    'name': pd.Series(dtype='string'),
    'address': pd.Series(dtype='string'),
    'phone_number': pd.Series(dtype='string'),
    'email': pd.Series(dtype='string'),
    'savings_account_id': pd.Series(dtype='string'),
    'card_id': pd.Series(dtype='string'),
    'active_record': pd.Series(dtype='bool'),
    'updated_ts': pd.Series(dtype='datetime64[ns]'),
})
savings_accounts_denormalize_df = pd.DataFrame({
    'id': pd.Series(dtype='string'),
    'op': pd.Series(dtype='string'),
    'ts': pd.Series(dtype='int64'),
    'savings_account_id': pd.Series(dtype='string'),
    'balance': pd.Series(dtype='int64'),
    'interest_rate_percent': pd.Series(dtype='float64'),
    'status': pd.Series(dtype='string'),
    'active_record': pd.Series(dtype='bool'),
    'updated_ts': pd.Series(dtype='datetime64[ns]'),
})

cards_denormalize_df = processing.execute('id', cards_df, cards_denormalize_df)
accounts_denormalize_df = processing.execute('id', accounts_df, accounts_denormalize_df)
savings_accounts_denormalize_df = processing.execute('id', saving_accounts_df, savings_accounts_denormalize_df)

cards_denormalize_df['ts_to_datetime'] = pd.to_datetime(cards_denormalize_df['ts'], unit='ms')
accounts_denormalize_df['ts_to_datetime'] = pd.to_datetime(accounts_denormalize_df['ts'], unit='ms')
savings_accounts_denormalize_df['ts_to_datetime'] = pd.to_datetime(savings_accounts_denormalize_df['ts'], unit='ms')

cards_denormalize_df = cards_denormalize_df.rename(columns={'status': 'card_status'})
savings_accounts_denormalize_df = savings_accounts_denormalize_df.rename(columns={'status': 'saving_account_status'})

print('\nCards')
print(cards_denormalize_df)
print('\nAccounts')
print(accounts_denormalize_df)
print('\nSaving Accounts')
print(savings_accounts_denormalize_df)


denormalize_df = pd.DataFrame({
    'account_id': pd.Series(dtype='string'),
    'name': pd.Series(dtype='string'),
    'address': pd.Series(dtype='string'),
    'phone_number': pd.Series(dtype='string'),
    'email': pd.Series(dtype='string'),
    'savings_account_id': pd.Series(dtype='string'),
    'card_id': pd.Series(dtype='string'),
    'card_number': pd.Series(dtype='string'),
    'credit_used': pd.Series(dtype='int64'),
    'monthly_limit': pd.Series(dtype='int64'),
    'card_status': pd.Series(dtype='string'),
    'savings_account_id': pd.Series(dtype='string'),
    'balance': pd.Series(dtype='int64'),
    'interest_rate_percent': pd.Series(dtype='float64'),
    'saving_account_status': pd.Series(dtype='string'),
    'active_record': pd.Series(dtype='bool'),
    'updated_ts': pd.Series(dtype='datetime64[ns]'),
})
print('\nDenormalize tables joined')
exclude_columns = ['id', 'op', 'ts', 'active_record', 'updated_ts', 'ts_to_datetime']
for index, row in cards_denormalize_df.iterrows():
    tmp_df = denormalize_df[(denormalize_df['card_id'] == row['card_id']) & (denormalize_df['active_record'] == True)]
    index_tmp_df = denormalize_df[(denormalize_df['card_id'] == row['card_id']) & (denormalize_df['active_record'] == True)].index

    cards = pd.DataFrame.from_records([row.to_dict()])
    include_columns = list(set(cards.columns) - set(exclude_columns))
    cards = cards[include_columns]
    record = cards.to_dict(orient='records')[0]

    accounts = accounts_denormalize_df[
        (accounts_denormalize_df['ts'] <= row.to_dict().get('ts'))
        & (accounts_denormalize_df['card_id'] <= row.to_dict().get('card_id'))
    ].tail(1)
    if not accounts.empty:
        include_columns = list(set(accounts.columns) - set(exclude_columns))
        accounts = accounts[include_columns]
        record.update(accounts.to_dict(orient='records')[0])

    savings_accounts = savings_accounts_denormalize_df[
        (savings_accounts_denormalize_df['ts'] <= row.to_dict().get('ts'))
        & (savings_accounts_denormalize_df['savings_account_id'] <= record.get('savings_account_id'))
    ].tail(1)
    if not savings_accounts.empty:
        include_columns = list(set(savings_accounts.columns) - set(exclude_columns))
        savings_accounts = savings_accounts[include_columns]
        record.update(savings_accounts.to_dict(orient='records')[0])

    if tmp_df.empty:
        fill_cols = [col for col in denormalize_df.columns if col not in record.keys()]
        time.sleep(1)
        for col in fill_cols:
            if col == 'active_record':
                record.update({'active_record': True})
            elif col == 'updated_ts':
                record.update({'updated_ts': datetime.now()})

        record = pd.DataFrame.from_records([record])
        denormalize_df = pd.concat([denormalize_df, record]).reset_index(drop=True)
    else:
        fill_cols = [col for col in denormalize_df.columns if col not in record.keys()]
        time.sleep(1)
        for col in fill_cols:
            if col == 'active_record':
                record.update({'active_record': True})
                tmp_df['active_record'] = False
            elif col == 'updated_ts':
                record.update({'updated_ts': datetime.now()})
            else:
                record.update(tmp_df[[col]].to_dict(orient='records')[0])

        record = pd.DataFrame.from_records([record])
        denormalize_df.loc[index_tmp_df, :] = tmp_df[:]
        denormalize_df = pd.concat([denormalize_df, record]).reset_index(drop=True)

print(denormalize_df)

print('\nTransaction has been made')
print(denormalize_df[denormalize_df['credit_used'] > 0].groupby(['account_id'])['credit_used'].agg('count').reset_index(name='total_transaction'))
