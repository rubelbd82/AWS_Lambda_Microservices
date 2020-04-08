import json

# Required imports
import botocore
import boto3

# Update your cluster and secret ARNs
cluster_arn = 'your-own-cluser-arn' 
secret_arn = 'your-own-secret-arn'

def lambda_handler(req, context):
    try:
        records = call_rds_data_api(req['latitude'], req['longitude'], 30, req['conditionReal'], req['rowId']);
        return {
            'status': 200,
            'message' : 'Information retrieved successfully',
            'data' : records
            }
    except Exception as e:
        print('Exception: ')
        print(e)
        return {
            'status': 500,
            'message' : 'Failed to load data'
            }

def call_rds_data_api(lat, lng, distance, conditionReal, rowId):
    print('from call_rds_data_api api');
    rds_data = boto3.client('rds-data')
    
    sql = '';
    
    if (conditionReal):
        sql = """
        SELECT
            id, (
            6371 * acos (
              cos ( radians(:lat) )
              * cos( radians( latitude ) )
              * cos( radians( longitude ) - radians(:lng) )
              + sin ( radians(:lat) )
              * sin( radians( latitude ) )
            )
          ) AS distance
        FROM corona
        WHERE testPositive = TRUE and id != :rowId
        HAVING distance < :distance
        ORDER BY distance
        LIMIT 0 , 20;
        
              """
    else:
        sql = """
        SELECT
            id, (
            6371 * acos (
              cos ( radians(:lat) )
              * cos( radians( latitude ) )
              * cos( radians( longitude ) - radians(:lng) )
              + sin ( radians(:lat) )
              * sin( radians( latitude ) )
            )
          ) AS distance
        FROM corona
        WHERE testPositive = FALSE and id != :rowId and (symptomCough = TRUE OR symptomFever=TRUE OR symptomChestPain = TRUE OR symptomSmell = TRUE)
        HAVING distance < :distance
        ORDER BY distance
        LIMIT 0 , 20;
          """
    
          
    param_lat = {'name':'lat', 'value':{'doubleValue': lat}}
    param_lng = {'name':'lng', 'value':{'doubleValue': lng}}
    param_distance = {'name':'distance', 'value':{'longValue': distance}}
    param_rowId = {'name':'rowId', 'value':{'longValue': rowId}}
    
    param_set = [param_lat, param_lng, param_distance, param_rowId]

    response = rds_data.execute_statement(
        resourceArn = cluster_arn, 
        secretArn = secret_arn, 
        database = 'widmobile', 
        sql = sql,
        parameters = param_set)
    
    return response['records']
