import pandas as pd
import re
import os

def data_cleaner():
   airflow_home = os.environ["AIRFLOW_HOME"]
   df = pd.read_csv(airflow_home+'/dags/repo/store_files/raw_store_transactions.csv')
   
   def clean_store_location(st_loc):
    return re.sub(r'[^\w\s]', '', st_loc).strip()
           
    
    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id
		
    def remove_dollar(amount):
        return float(amount.replace('$', ''))
        
    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))
    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
        
    df.to_csv('/tmp/clean_store_transactions.csv', index=False)


