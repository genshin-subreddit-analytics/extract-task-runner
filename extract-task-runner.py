import os
import boto3

client = boto3.client('ecs')

cluster_name = os.environ['CLUSTER_NAME']
task_definition = os.environ['TASK_DEFINITION']


def lambda_handler(event, context):
    try:
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
                                'value': 'arknights'
                            }
                        ]
                    }
                ]
            })
        print(response)
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

