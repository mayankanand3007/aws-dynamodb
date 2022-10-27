'''
    @Author: Mayank Anand
    @Date Created: 27-10-2022
    @Last Modified by: Mayank Anand
    @Last Modified date: 27-10-2022
    @Title: Menu driven CRUD Operations in AWS DynamoDB
'''

# Importing libraries
from pprint import pprint
import boto3
from botocore.exceptions import ClientError


def create_movie_table(client):
    """
        Description: Creates Movie Table inside DynamoDB Table with the given attributes.
        Parameter: 
            client: Boto3 DynamoDB resource to perform operations inside DynamoDB.
        Return: Table created inside DynamoDB.
    """
    table = client.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


def put_movie(client, title, year, plot, rating):
    """
        Description: Insert items inside Movie DynamoDB Table with the given attributes.
        Parameter: 
            client: Boto3 DynamoDB resource to perform operations inside DynamoDB.
            title: Title of the movie to be added.
            year: Year of the movie which is being added.
            plot: Plot of the movie which is being added.
            rating: Rating of the movie which is being added.
        Return: Response if the table is added or not.
    """
    response = client.put_item(
       TableName='Movies',
       Item={
            'year': {
                'N': "{}".format(year),
            },
            'title': {
                'S': "{}".format(title),
            },
            'plot': {
                "S": "{}".format(plot),
            },
            'rating': {
                "N": "{}".format(rating),
            }
        }
    )
    return response


def get_movie(client, title, year):
    """
        Description: Retrieving items inside Movie DynamoDB Table with the given attributes.
        Parameter: 
            client: Boto3 DynamoDB resource to perform operations inside DynamoDB.
            title: Title of the movie to be fetched.
            year: Year of the movie which is being fetched.
        Return: Response if the table is there or not.
    """
    try:
        response = client.get_item(       
                TableName='Movies',
                Key={
                        'year': {
                                'N': "{}".format(year),
                        },
                        'title': {
                                'S': "{}".format(title),
                        }
                    }
                )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


def update_movie(client, title, year, rating, plot, actors):
    """
        Description: Update items inside Movie DynamoDB Table with the given attributes.
        Parameter: 
            client: Boto3 DynamoDB resource to perform operations inside DynamoDB.
            title: Title of the movie to be updated.
            year: Year of the movie which is being updated.
            rating: Rating of the movie which is being updated.
            plot: Plot of the movie which is being updated.
            actors: Actors of the movie which is being updated.
        Return: Response if the table is updated or not.
    """
    response = client.update_item(
        TableName='Movies',
        Key={
            'year': {
                    'N': "{}".format(year),
            },
            'title': {
                    'S': "{}".format(title),
            }
        },
        ExpressionAttributeNames={
            '#R': 'rating',
            '#P': 'plot',
            '#A': 'actors'
        },
        ExpressionAttributeValues={
            ':r': {
                'N': "{}".format(rating),
            },
            ':p': {
                'S': "{}".format(plot),
            },
            ':a': {
                'SS': actors,
            }
        },
        UpdateExpression='SET #R = :r, #P = :p, #A = :a',
        ReturnValues="UPDATED_NEW"
    )
    return response


def delete_underrated_movie(client, title, year, rating):
    """
        Description: Delete items inside Movie DynamoDB Table with the given attributes.
        Parameter: 
            client: Boto3 DynamoDB resource to perform operations inside DynamoDB.
            title: Title of the movie to be deleted.
            year: Year of the movie which is being deleted.
            rating: Rating of the movie which is being deleted.
        Return: Response if the table is deleted or not.
    """
    try:
        response = client.delete_item(
            TableName='Movies',
            Key={
                'year': {
                    'N': "{}".format(year),
                },
                'title': {
                    'S': "{}".format(title),
                }
            },
            ConditionExpression="rating <= :a",
            ExpressionAttributeValues={
                ':a': {
                    'N': "{}".format(rating),
                }
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response 

if __name__ == '__main__':
    # s3 = boto3.resource(service_name = 'dynamodb', region_name = 'ap-south-1', aws_access_key_id = 'AKIAQEWE5Q3SHWV3FLWR', \
    # aws_secret_access_key = '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'
    # )
    # Getting DynamoDB resource using Boto3.
    client = boto3.client('dynamodb', region_name = 'ap-south-1', aws_access_key_id = 'AKIAQEWE5Q3SP7YDCWUW', \
        aws_secret_access_key = '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')
    while True:
        print("Welcome to CRUD Operations in DynamoDB using Boto3")
        print_stmts = ["Create Movies Table", "Insert Item in Movie Table", "Get items from Movie Table", "Update item in Movie Table", 
                "Delete an Item in Movie Table"]
        for print_stmt in range(len(print_stmts)):
            print(f"{print_stmt + 1} - {print_stmts[print_stmt]}")
        # Asks user for input from the above options.
        operation_number = int(input("Enter the above number(1-5) to do the following operation: "))
        if operation_number == 1:
            ## Create DynamoDB
            movie_table = create_movie_table(client)
            print("Creating Movies Table succeeded.")
            print("Table status:{}".format(movie_table))
        elif operation_number == 2:
            ## Insert in to DynamoDB
            movie_resp = put_movie(client, "Black Adam", 2022, "DC's new movie starring Dwayne Johnson.", 7.1)
            print("Insert in to Movies table succeeded.")
            pprint(movie_resp, sort_dicts=False)
        elif operation_number == 3:
            ## Get an item from DynamoDB
            movie = get_movie(client, "Black Adam", 2022)
            if movie:
                print("Getting an item from Movie Table succeeded.")
                pprint(movie, sort_dicts=False)
        elif operation_number == 4:
            ## Update and item in  DynamoDB
            update_response = update_movie(client, "Black Adam", 2022, 7.1, "DC's new movie starring Dwayne Johnson.", \
                ["Dwayne Johnson", "Sarah Shahi", "Henry Cavill"])
            print("Updated item in Movie Table succeeded.")
            pprint(update_response, sort_dicts=False)
        elif operation_number == 5:
            ## Delete an Item in DynamoDB table
            delete_response = delete_underrated_movie(client, "Black Adam", 2022, 7.1)
            if delete_response:
                print("Deleting an Item in Movie table succeeded.")
                pprint(delete_response, sort_dicts=False)
        else:
            print("Invalid number entered. Please try again: ")
            continue
        # Checks if user wants to end the loop of performing operations on DynamoDB Table.
        if input('Do you want to perform operations on DynamoDB Table again?(y/n): ') != 'y':
            break