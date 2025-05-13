from unittest.mock import patch
import unittest
import json, pytest
import fakesnow, pandas as pd
from datetime import datetime, timedelta

from snowflake import snowpark
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import *
import snowflake.connector

from project.twitter_rda import read_from_sf, main, transform_data, fetch_data, load_data
from conftest import get_tweet_data, get_transformed_tweet_data, get_transformed_tweet_data_invalid

current_date = datetime.utcnow().strftime("%Y%m%d")
table_id = f"{current_date[2:]}"
gcs_bucket_id = (datetime.utcnow() - timedelta(1)).strftime("%Y%m%d")[2:]
dataset = "twitter"
data_type = "tweets"
document_table = f"{data_type}_{table_id}"



def test_read_from_sf_count(live_session):
    current_database = live_session.get_current_database()
    current_schema = live_session.get_current_schema()
    document_table_name = document_table
    input_loc = f'/testing/sample_files/'

    try:
        result = read_from_sf(live_session, document_table_name, input_loc, schema = current_schema, temp_table = False)

        record_file_query = f'''
            SELECT 
                $1 AS DATA
            FROM 
                @{current_schema}.GCP_TWITTER_STAGE{input_loc} 
                (file_format => {current_schema}.JSON_FORMAT_RDA)
            ORDER BY 1
        '''
        record_file_df = live_session.sql(record_file_query)

        # print(len(result))
        assert len(result) > 0, 'No data loaded into the table'
        assert len(result) == record_file_df.count()
        
    finally:
        pass
        # live_session.sql(f"DROP TABLE IF EXISTS {current_schema}.{document_table_name}_temp").collect()



def test_read_from_sf_match_record(live_session):
    current_database = live_session.get_current_database()
    current_schema = live_session.get_current_schema()
    document_table_name = document_table
    input_loc = f'/testing/sample_files/'

    try:
        result = read_from_sf(live_session, document_table_name, input_loc, schema = current_schema, temp_table = False)

        record_file_query = f'''
            SELECT 
                $1 AS DATA
            FROM 
                @{current_schema}.GCP_TWITTER_STAGE{input_loc} 
                (file_format => {current_schema}.JSON_FORMAT_RDA)
            ORDER BY 1
        '''
        record_file = live_session.sql(record_file_query).collect()
        record_table = live_session.sql(f'SELECT * FROM {current_schema}.{document_table_name}_temp ORDER BY 1').collect()

        for i in range(len(result)):
            assert json.loads(record_file[i]['DATA']) == json.loads(record_table[i]['RAW_DATA'])
        
    finally:
        pass
        # live_session.sql(f"DROP TABLE IF EXISTS {current_schema}.{document_table_name}_temp").collect()


# # # checking if the transformation logic is working fine or not
def test_transform_data(local_session):
    get_tweet_data_lst = get_tweet_data()

    for i in get_tweet_data_lst:
        x = transform_data(local_session,i)
        if 'data' not in i.keys():
            assert x ==  None
        elif 'connection_issue' in i.keys():
            assert x ==  None
        else:
            assert len(x.keys()) == 19
            assert x.get('timezone') == 'UTC'
            assert x.get('tweet_id') == 1234567890
            assert x.get('created_at') == '2023-10-26 12:00:00'
            assert x.get('user_id') == 987654321
            assert x.get('tweet') == 'This is a sample tweet! #Snowflake'
            assert x.get('mentions') == ['user1','user2']
            assert x.get('photos') == ['https://example.com/image.jpg']
            assert x.get('link') == 'https://twitter.com/test_user/status/1234567890'



# # # checking if all the input data is being transformed or not
@patch('project.twitter_rda.transform_data')
def test_fetch_data(mock_transform_data, local_session):
    mock_data = [{'a':1,'b':2}, None]
    
    for i in mock_data:
        mock_transform_data.return_value = i

        get_tweet_data_lst = get_tweet_data()
        res = fetch_data(local_session, get_tweet_data_lst, 1)
        actual = [i for i in res]

        # print(actual)
        if i == None:
            assert len(actual) == 0
            assert actual == []
        else:
            assert len(actual) == len(get_tweet_data_lst) # len of output list should be equal to length of input data
            for i in range(len(mock_data)):
                assert actual[i] == {'a':1,'b':2}



def test_load_data(live_session):
    current_database = live_session.get_current_database()
    current_schema = live_session.get_current_schema()

    try:
        res = load_data(live_session,get_transformed_tweet_data(),document_table, schema=current_schema, temp_table = False)

        # print(res)
        df = live_session.table(f"{current_schema}.{document_table}")
        data = df.collect()

        assert df.count() == 2
        for i in data:
            assert i['TWEET_ID'] in ('1234567890', '9876543211')
            if i['TWEET_ID'] == '1234567890':
                assert i['CONVERSATION_ID'] == '1234567890'
                assert i['CREATED_AT'] == '2023-10-26 12:00:00'
                assert i['USER_ID'] == '987654321'
                assert json.loads(i['MENTIONS']) == ['user1', 'user2']
                assert i['RETWEET_COUNT'] == 5

            else:
                assert i['CONVERSATION_ID'] == '9876543211'
                assert i['CREATED_AT'] == '2023-10-27 11:10:01'
                assert i['USER_ID'] == '543210987'
                assert json.loads(i['MENTIONS']) == ['user11', 'user22']
                assert i['RETWEET_COUNT'] == 50
    finally:
        pass
        # live_session.sql(f"DROP TABLE IF EXISTS {current_schema}.{document_table}").collect() 








# def test_load_data():
#     with fakesnow.server() as conn_kwargs:
#         session_configs = conn_kwargs.copy()
#         session_configs['database'] = "GROWTH_PROTOCOL_DEV_DB" 
#         session_configs['schema'] = "RAW_TWITTER"

#         with snowflake.connector.connect(**conn_kwargs) as conn:
#             conn.cursor().execute('CREATE DATABASE IF NOT EXISTS GROWTH_PROTOCOL_DEV_DB;')
#             conn.cursor().execute('USE DATABASE GROWTH_PROTOCOL_DEV_DB;')
#             conn.cursor().execute('CREATE SCHEMA GROWTH_PROTOCOL_DEV_DB.RAW_TWITTER;')
#             # conn.cursor().execute('CREATE OR REPLACE TABLE test_twitter (id integer, name string);')
            
#             # session = snowpark.Session(conn)
#             # conn.commit()
            
#         fakesnow_session = snowpark.Session.builder.configs(session_configs).create()

#         fakesnow_session.sql("use database GROWTH_PROTOCOL_DEV_DB").collect()
#         fakesnow_session.sql("use schema RAW_TWITTER").collect()
#         fakesnow_session.sql('CREATE OR REPLACE TABLE GROWTH_PROTOCOL_DEV_DB.RAW_TWITTER.test_twitter (id integer, name string);').collect()
#         x = fakesnow_session.sql('select CURRENT_DATABASE();').collect()
#         print(x)
            

#             # def mock_sql_table(t):
#             #     return session.create_dataframe([None])
            
#             # session.sql = mock_sql_table

#             # res = load_data(session,[{"id":1,"name":"user1"},{"id":2,"name":"user2"}],'test_twitter')
#         # pandas_df = pd.DataFrame([(1, 'user1'), (3, 'user2')])
#         # snowpark_df = fakesnow_session.create_dataframe(pandas_df)

#         df = fakesnow_session.create_dataframe([(1, 'user1'), (3, 'user2')], schema = ["id", "name"])
#         df.write.mode("overwrite").save_as_table(f"GROWTH_PROTOCOL_DEV_DB.RAW_TWITTER.test_twitter")

          
#         print(fakesnow_session.table('GROWTH_PROTOCOL_DEV_DB.RAW_TWITTER.test_twitter').collect())
#         print(df.collect())
        # print(session.table('test_twitter').collect())

            


# def test_load_data_invalid(live_session):
#     current_database = live_session.get_current_database()
#     current_schema = live_session.get_current_schema()
#     try:
#         with pytest.raises(Exception):    
#             res = load_data(live_session,get_transformed_tweet_data_invalid(),document_table, schema=current_schema, temp_table = False)
#             # print(res)
#     finally:
#         pass
#         live_session.sql(f"DROP TABLE IF EXISTS {current_schema}.{document_table}").collect() 




# def test_read_from_sf_invalid_file(live_session):
#     current_database = live_session.get_current_database()
#     current_schema = live_session.get_current_schema()
#     document_table_name = document_table
#     input_loc = f'/testing/incorrect_files/'

#     try:
#         with pytest.raises(Exception):    
#             x= read_from_sf(live_session, document_table_name, input_loc, schema = current_schema, temp_table = False)
#             # print(x)
#     finally:
#         live_session.sql(f"DROP TABLE IF EXISTS {document_table_name}_temp").collect() 
