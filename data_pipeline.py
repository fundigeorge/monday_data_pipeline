import pandas as pd
import csv
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

#ETL scheduling with airflow DAG
default_args = {
    'owner': 'fundi',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 2, 22, 52),
    'email': ['fundigeorge@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'customers_purchases',
    default_args=default_args,
    description='ETL pipeline for customer purchases',
    schedule_interval=timedelta(seconds=10),
)

path_purchases1 = '/home/fundi/moringaschool/week7/monday/dataset1_202302/dataset1.csv'
path_purchases2 = '/home/fundi/moringaschool/week7/monday/dataset1_202302/dataset2.csv'
path_purchases_returns = '/home/fundi/moringaschool/week7/monday/dataset1_202302/dataset3.csv'

def read_csv(file):
    #read the file using csv module to pandas dataframe
    data_list = []
    with open(file) as f:
        reader = csv.DictReader(f)
        for d in reader:
            data_list.append(d)
    data_df = pd.DataFrame(data_list)
    return data_df
def dataset1_cleaning(dataset1:pd.DataFrame, dataset2:pd.DataFrame):
    #standardise columns names by removing any empty space, lowercase
    dataset1.columns = dataset1.columns.str.replace(' ', '_').str.lower()
    #expect missing data on payment_status, and promo_code
    dataset1.loc[dataset1['promo_code'].isna(), 'promo_code'] = 'NO PROMO'
    dataset1.loc[dataset1['payment_status'].isna(), 'payment_status'] = 'Unknown'
    #rearrange columns and add late_payment_fee column to conform to dataset2
    dataset1 =dataset1[['customer_id', 'date_of_purchase', 'total_amount_billed',
       'payment_status', 'payment_method', 'country_of_purchase', 'promo_code']]
    #dataset1.loc[:, 'late_payment_fee'] = 0
    dataset1 = dataset1.merge(dataset2.loc[:, ['customer_id', 'date_of_payment', 'country_of_payment','late_payment_fee']],
                               how='left', 
                              left_on=['customer_id', 'date_of_purchase','country_of_purchase'],
                               right_on=['customer_id', 'date_of_payment', 'country_of_payment'])
    print(dataset1)
    return dataset1

def dataset2_cleaning(data:pd.DataFrame,dataset1_col):
    #standardise columns names by removing any empty space, lowercase
    data.columns = data.columns.str.replace(' ', '_').str.lower()
    #expect missing data on payment_status,
    data.loc[data['payment_status'].isna(), 'payment_status'] = 'Unknown'
    #conform dataset1 to dateset2 columns
    data.loc[:, 'promo_code'] = 'NO PROMO'
    #rearrange columns on dataset1
    data = data[['customer_id', 'date_of_payment', 'amount_paid', 'payment_status', 'payment_method',
       'country_of_payment', 'promo_code', 'late_payment_fee']]
    #conform columns on dataset2 to dataset1 columns
    data.columns = dataset1_col
    return data


def dataset1_dataset2_merge(dataset1:pd.DataFrame, dataset2:pd.DataFrame):
    #standardise columns names by removing any empty space, lowercase
    dataset1.columns = dataset1.columns.str.replace(' ', '_').str.lower()
    dataset2.columns = dataset2.columns.str.replace(' ', '_').str.lower()
    #rename columns to be used as keys
    dataset2.rename(columns={'date_of_payment':'date_of_purchase', 'country_of_payment':'country_of_purchase'}, inplace=True)
    #vlook up late_payment_fee on dataset1 from dataset2
    dataset1 = dataset1.merge(dataset2.loc[:, ['customer_id', 'date_of_purchase','country_of_purchase','late_payment_fee']],
                               how='left', 
                              on=['customer_id', 'date_of_purchase','country_of_purchase'])
    
    #vlookup promo code on dataset2 from dataset1
    dataset2 = dataset2.merge(dataset1.loc[:, ['customer_id', 'date_of_purchase','country_of_purchase','promo_code']],
                               how='left', 
                              on=['customer_id', 'date_of_purchase','country_of_purchase'])
    #Match the column order on both datasets
    dataset1 = dataset1[['customer_id', 'date_of_purchase','country_of_purchase', 'total_amount_billed',
                        'payment_status', 'payment_method', 'promo_code', 'late_payment_fee']]
    dataset2 = dataset2[['customer_id','date_of_purchase','country_of_purchase', 'amount_paid', 'payment_status', 
                         'payment_method', 'promo_code', 'late_payment_fee']]
    #Match the column names
    dataset1.columns = dataset2.columns

    #concatenate dataset1 and dataset2
    dataset = pd.concat([dataset1, dataset2], ignore_index=True)

    #expect missing data on payment_status, and promo_code
    dataset.loc[dataset['promo_code'].isna(), 'promo_code'] = 'NO PROMO'
    dataset.loc[dataset['late_payment_fee'].isna(), 'late_payment_fee'] = 0
    dataset.loc[dataset['payment_status'].isna(), 'payment_status'] = 'Unknown'
    #remove duplicates
    dataset = dataset.drop_duplicates(subset=['customer_id', 'date_of_purchase','country_of_purchase'])
    return dataset

def purchases_refund_merge(purchases:pd.DataFrame, refunds:pd.DataFrame):
    #merge refund and refund_reason
    customers = purchases.merge(refunds, how='left', 
                                left_on=['customer_id', 'country_of_purchase'],
                                right_on=['customer_id', 'country_of_refund'],
                                )
    #drop duplicated
    customers = customers.drop(columns=['country_of_refund'])
    #replace missing values on reason_for_refund and on other columns due to vlookup
    customers.loc[customers['reason_for_refund'].isna(), 'reason_for_refund'] = 'no reason'
    customers.loc[customers['refund_amount'].isna(), 'refund_amount'] = 0
    customers.loc[customers['date_of_refund'].isna(), 'date_of_refund'] = customers['date_of_purchase']

    return customers

def transform_data(data:pd.DataFrame):
    #convert data type
    data['date_of_purchase'] = pd.to_datetime(data['date_of_purchase'], format='%m/%d/%Y')
    data['date_of_refund'] = pd.to_datetime(data['date_of_refund'], format='%m/%d/%Y')
    data.sort_values(by=['customer_id', 'date_of_purchase'])  

    #check for outlier
    #get the lower and upper bound
    q1 = np.percentile(data['amount_paid'], 25)
    q3 = np.percentile(data['amount_paid'],75)
    iqr = q3-q1
    lower_bound = iqr - 1.5*iqr
    upper_bound = iqr + 1.5*iqr
    #filter out the outliers if any
    data = data.loc[(data['amount_paid']>lower_bound)&(data['amount_paid']<upper_bound),]
 
    return data

#load data into postgres db
def load_to_postgres(data:pd.DataFrame):
    conn = psycopg2.connect(host='localhost', port=5432, database = 'customers', user='postgres', password='fundi')
    cur = conn.cursor()
    #create table
    cur.execute('''create table if not exists customers_purchases 
                (customer_id integer, date_of_purchase date, country_of_purchase text, 
                amount_paid integer, payment_status text, payment_method text, promo_code text, 
                late_payment_fee integer, date_of_refund date, refund_amount integer, reason_for_refund text)''')
    #load data to sql
    engine = create_engine('postgresql+psycopg2://postgres:fundi@localhost:5432/customers')
    data.to_sql(name = 'customers_purchases', con= engine, index=False, if_exists='append')
    conn.commit()
    #close cursor and connection
    cur.close()
    conn.close()

#validate the data
def validate_loaded_data():
    #connect to database
    conn = psycopg2.connect(host='localhost', port=5432, database = 'customers', user='postgres', password='fundi')
    cur = conn.cursor()
    #fetch records where amount paid >100
    cur.execute('select customer_id, country_of_purchase, amount_paid from customers_purchases where amount_paid >100')
    results = cur.fetchall()
    #close cursor and connection
    cur.close()
    conn.close()
    
    data = pd.DataFrame(results, columns=['customer_id', 'country_of_purchase', 'amount_paid'])
    return data


if __name__ == "__main__":
    #read the data from csv to pandas dataframe
    purchases1 = pd.read_csv(path_purchases1)
    purchases2 = pd.read_csv(path_purchases2)
    purchases_returns = pd.read_csv(path_purchases_returns)

    #data cleaning, transformations and merging
    '''
    ->dataset1 and dataset2 have similar info columns with different names which needs to be conformed
    ->dataset1 and dataset2 have almost similar info and they can be concatenated while dataset3 has additional
    info on refund to customer which can be vlooked up into dataset1 and dataset 2 (after concatenating dataset 1 and 2)
    ->dataset1 has promo code info missing in dataset 2 while dataset2 has late_payment_fee missing from dataset1
    which can be vlooked up from each other
    '''
    purchase = dataset1_dataset2_merge(purchases1, purchases2)
    
    #merge customer refunds to customer purchases
    customers = purchases_refund_merge(purchase, purchases_returns)

    #tranform data, convert datetime, check for outliers in amount paid
    customers = transform_data(customers)
    print(customers.columns)
    
    #load data into postgres db
    load_to_postgres(customers)
    
    #validate the data
    sample_data = validate_loaded_data()
    print(sample_data)

    #schedule ETL with apache airflow
    dataset_merge = PythonOperator(
        task_id='dataset1_dataset2_merge',
        python_callable=dataset1_dataset2_merge,
        dag=dag,
    )
    refund_merge = PythonOperator(
        task_id='purchases_refund_merge',
        python_callable=purchases_refund_merge,
        dag=dag,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        dag=dag,
    )

    validate = PythonOperator(
        task_id='validate_loaded_data',
        python_callable=validate_loaded_data,
        dag=dag,
    )

    dataset_merge>>refund_merge>>transform>>load>>validate
