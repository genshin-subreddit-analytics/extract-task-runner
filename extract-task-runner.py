import os
import boto3

client = boto3.client('ecs')
dynamodb = boto3.resource('dynamodb')

cluster_name = os.environ['CLUSTER_NAME']
task_definition = os.environ['TASK_DEFINITION']

table_name = os.environ['DB_TABLE_NAME']
pk_name = os.environ['DB_TABLE_PRIMARY_KEY']

def get_pk_value(table_name, pk_name, dynamodb_resource):
    table = dynamodb_resource.Table(table_name)

    # Define the query parameters
    query_params = {
        'ProjectionExpression': '#pk',
        'ExpressionAttributeNames': {'#pk': pk_name}
    }

    # Execute the query and get the primary key values
    primary_keys = []
    response = table.scan(**query_params)
    primary_keys.extend([item[pk_name] for item in response['Items']])

    # If there are more items to scan, continue scanning
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **query_params)
        primary_keys.extend([item[pk_name] for item in response['Items']])

    # Print the primary key values
    return primary_keys

def run_ecs_fargate(cluster_name, task_definition, subreddit):
    response = client.run_task(
        cluster=cluster_name,
        launchType='FARGATE',
        taskDefinition=task_definition,
        count=1,
        platformVersion='LATEST',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    os.environ['SUBNET_1'],
                    os.environ['SUBNET_2'],
                    os.environ['SUBNET_3']
                ],
                'securityGroups': [
                    os.environ['SECURITY_GROUP_1'],
                ],
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': cluster_name,
                    'environment': [
                        {
                            'name': 'SUBREDDIT_NAME',
                            'value': subreddit
                        },{
                            'name': 'CLIENT_TZ',
                            'value': os.environ['CLIENT_TZ']
                        },{
                            'name': 'DB_TABLE_NAME',
                            'value': table_name
                        },{
                            'name': 'DB_TABLE_PRIMARY_KEY',
                            'value': pk_name
                        },{
                            'name': 'DB_LAST_ARCHIVED_ATTR_NAME',
                            'value': os.environ['DB_LAST_ARCHIVED_ATTR_NAME']
                        }
                    ]
                }
            ]
        })
    return response


def lambda_handler(event, context):
    try:
        for subreddit in get_pk_value(table_name, pk_name, dynamodb):
            task_ = run_ecs_fargate(cluster_name, task_definition, subreddit)
            print(task_)
        return {
            'statusCode': 200,
            'body': "OK"
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
