from flask import Flask, redirect, url_for, render_template, request, jsonify
import subprocess, os, boto3, botocore
from botocore.client import Config
from botocore.exceptions import ClientError
import json, sys, argparse

# Set region
REGION = "us-east-1"

#the following lines from line 13-106 are taken and completed from mpcs_ec2_monitor.py, the in-class group exercise

def main(argv=None):
# Connect to SQS and get the message queue

# Handle command-line arguments for AWS credentials and resource names
    parser = argparse.ArgumentParser(
        description="Process AWS resources and credentials."
    )
    parser.add_argument(
        "--queue",
        action="store",
        dest="sqs_queue_name",
        required="true",
        help="SQS queue for storing AutoScaling notification messages",
    )
    parser.add_argument(
        "--table",
        action="store",
        dest="db_table_name",
        required="true",
        help="DynamoDB table where instance information is stored",
    )
    parser.add_argument(
        "--bucket",
        action="store",
        dest="s3_output_bucket",
        required="true",
        help="S3 bucket where list of instances will be stored",
    )
    parser.add_argument(
        "--key",
        action="store",
        dest="s3_output_key",
        required="true",
        help="S3 key where list of instances will be stored",
    )
    args = parser.parse_args()

    # Set queue names
    sqs_queue_name = args.sqs_queue_name
    db_table_name = args.db_table_name

    # Get S3 bucket and object
    s3_output_bucket = args.s3_output_bucket
    s3_output_key = args.s3_output_key

    print(
        f"Connecting to SQS queue {sqs_queue_name} and DynamoDB table {db_table_name}"
    )

    # Connect to SQS and get queue
    # cf. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#service-resource
    try:
        sqs=boto3.resource('sqs', region_name=REGION)
        queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)

    except ClientError as error:
        print(error)

    # Poll the message queue in a loop 
    while True:
        
        # Attempt to read a message from the queue
        print("Asking SQS for up to 10 messages.")
        # Get messages
        try:
            messages=queue.receive_messages(
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20)
        except ClientError as error:
            print(error)

        if len(messages) > 0:
            print(f"Received {str(len(messages))} messages...")
            # Iterate each message

            for message in messages:
                # Parse JSON message
                # You will need to go two levels deep to get the embedded message
                msg_body = json.loads(json.loads(message.body)["Message"])

                # If message read, extract job parameters from the message body as before

                # Include below the same code you used in prior homework
                # Get the input file S3 object and copy it to a local file
                # Use a local directory structure that makes it easy to organize
                # multiple running annotation jobs

                # Launch annotation job as a background process

                try:
                # Extract job parameters from message body
                
                    input_file=msg_body['input_file_name']
                    job_id=msg_body['job_id']
                    s3_key_st=msg_body['s3_key_input_file']
               
                except Exception as error:
                    print(error)
                    continue
                
                path_to_job='/home/ubuntu/anntools/data/jobs/{}'.format(job_id)

                #I referenced geekforgeeks's 'Python | os.path.exists() method' page for the below if not check
                #https://www.geeksforgeeks.org/python-os-path-exists-method/
                if not os.path.exists(path_to_job):
                    #I referenced geekforgeek's 'Python | os.mkdir() method' page for the below method to make a directory
                    #https://www.geeksforgeeks.org/python-os-mkdir-method/
                    os.mkdir('/home/ubuntu/anntools/data/jobs/{}'.format(job_id))

                    try:
                        s3 = boto3.resource("s3")
                        destination='/home/ubuntu/anntools/data/jobs/{}/{}'.format(job_id, input_file)

                        # Get the input file S3 object and copy it to a local file

                        #I referenced 'Downloading a File' from Boto3 Docs 1.9.42 write-up to execute the below download_file function
                        #https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html

                        s3.Bucket('gas-inputs').download_file(s3_key_st, destination)


                    except ClientError as error:
                        print(error)
                        continue
                    except Exception as error:
                        print(error)
                        continue

                    try:
                        #run the processor
                        destination='/home/ubuntu/anntools/data/jobs/{}/{}'.format(job_id, input_file)
                        #I referenced the 'Popen Constructor' section in this Subprocess Python documentation to execute subprocess.Popen()
                        #https://docs.python.org/3/library/subprocess.html

                        # Launch annotation job as a background process

                        subprocess.Popen(['python', '/home/ubuntu/anntools/a9_run.py', destination, job_id])

                    except ClientError as error:
                        print(error)
                        continue
                    except Exception as error:
                        print(error)
                        continue

                    try: 
                        # update the item job_status to running
                        # I referenced the update_item() section of Dynamodb, Boto3 Docs 1.26.61 documentation to execute the below update_item
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item

                        client=boto3.resource('dynamodb', region_name='us-east-1')
                        table=client.Table("jchen201_annotations")
                        table.update_item(
                            Key={'job_id':job_id},
                            UpdateExpression='SET job_status = :val1', 
                            ConditionExpression='contains(job_status, :val2)',
                            ExpressionAttributeValues={
                                ':val1':'RUNNING',
                                ':val2': 'PENDING'
                            })


                    except ClientError as error:
                        print(error)
                        continue

                    try:
                        # Delete the message from the queue, if job was successfully submitted
                        print("Deleting message with job id: {}".format(job_id) )
                        message.delete()

                    except ClientError as error:
                        print(error)
                        continue
                    except Exception as error:
                        print(error)
                        continue



#I kept this part of the code from mpcs_ec2_monitor.py
if __name__ == "__main__":
    sys.exit(main())