import json

# Required imports
import botocore
import boto3

# Update your cluster and secret ARNs
cluster_arn = 'arn:aws:rds:your-own-cluster-arn' 
secret_arn = 'arn:aws:secretsmanager:your-own-secret-arn'

def lambda_handler(request, context):
    # x = req['body'].rstrip()
    try:
        db_response = call_rds_data_api(request['testPositive'], request['testNegative'], request['notTested'], request['symptomCough'], request['symptomFever'], request['symptomChestPain'], request['symptomSmell'], request['returnedFromAbroad'], request['olderThan50'], request['latitude'], request['longitude'], request['rowId']);
        
        return {
            'status': 200,
            'rowId': db_response,
            'message' : 'Your information saved successfully'
        }
    except Exception as e:
        print('Exception: ')
        print(e)
        return {
            'status': 500,
            'message' : 'Failed to save data'
            }


def call_rds_data_api(testPositive, testNegative, notTested, symptomCough, symptomFever, symptomChestPain, symptomSmell, returnedFromAbroad, olderThan50, latitude, longitude, rowId):
    print('from data api');
    rds_data = boto3.client('rds-data')

    sql = """
    
    INSERT INTO corona (testPositive, testNegative, notTested, symptomCough, symptomFever, symptomChestPain, symptomSmell, returnedFromAbroad, olderThan50, latitude, longitude) 
    VALUES (:testPositive, :testNegative, :notTested, :symptomCough, :symptomFever, :symptomChestPain, :symptomSmell, :returnedFromAbroad, :olderThan50, :latitude, :longitude);
    
          """
          
    if (rowId != 0) :
        sql = """
    UPDATE corona SET testPositive = :testPositive, testNegative = :testNegative, notTested = :notTested, symptomCough = :symptomCough, symptomFever = :symptomFever, symptomChestPain = :symptomChestPain, symptomSmell = :symptomSmell, returnedFromAbroad = :returnedFromAbroad, olderThan50 = :olderThan50, latitude = :latitude, longitude = :longitude 
    WHERE id = :rowId
          """
    param_testPositive = {'name':'testPositive', 'value':{'booleanValue': testPositive}}
    param_testNegative =  {'name':'testNegative', 'value':{'booleanValue': testNegative}}
    param_notTested =  {'name':'notTested', 'value':{'booleanValue': notTested}}
    param_symptomCough =  {'name':'symptomCough', 'value':{'booleanValue': symptomCough}}
    param_symptomFever  =  {'name':'symptomFever', 'value':{'booleanValue': symptomFever}}
    param_symptomChestPain =  {'name':'symptomChestPain', 'value':{'booleanValue': symptomChestPain}}
    param_symptomSmell =  {'name':'symptomSmell', 'value':{'booleanValue': symptomSmell}}
    param_returnedFromAbroad  =  {'name':'returnedFromAbroad', 'value':{'booleanValue': returnedFromAbroad}}
    param_olderThan50 =  {'name':'olderThan50', 'value':{'booleanValue': olderThan50}}
    param_latitude =  {'name':'latitude', 'value':{'doubleValue': latitude}}
    param_longitude =  {'name':'longitude', 'value':{'doubleValue': longitude}}
    param_rowId =  {'name':'rowId', 'value':{'longValue': rowId}}
    
    param_set = [param_testPositive, param_testNegative, param_notTested, param_symptomCough, param_symptomFever, param_symptomChestPain, param_symptomSmell, param_returnedFromAbroad, param_olderThan50, param_latitude, param_longitude, param_rowId]
 
    response = rds_data.execute_statement(
        resourceArn = cluster_arn, 
        secretArn = secret_arn, 
        database = 'widmobile', 
        sql = sql,
        parameters = param_set)
    
    if (rowId != 0) :
        return rowId
    else:
        return response['generatedFields'][0]['longValue']
