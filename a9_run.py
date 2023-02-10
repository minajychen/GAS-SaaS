# a9_run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import os
import logging
import boto3
from botocore.exceptions import ClientError
from flask import jsonify


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

            s3 = boto3.client("s3")

            file_path = sys.argv[1].split('.')[0]
            file_name=os.path.split(file_path)[1]

            job_id=sys.argv[2]

            s3_results_bucket='gas-results'

            complete_time=round(time.time())

            ann_key_name="jchen201" + "/" + "userx" + "/" + job_id + "~" + f'{file_name}.annot.vcf'
            log_key_name="jchen201" + "/" + "userx" + "/" + job_id + "~" + f'{file_name}.vcf.count.log'

            #I referenced the upload_file section of Boto3 Docs 1.26.61 documentation to execute upload_file
            #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

            try:
                s3.upload_file(f'{file_path}.annot.vcf', s3_results_bucket, ann_key_name)
                s3.upload_file(f'{file_path}.vcf.count.log', s3_results_bucket, log_key_name)
            except ClientError as e:
                print(e)
            except Exception as e:
                print(e) 
                
            # # Update item in dynamodb
            #I referenced the update_item() section of Dynamodb, Boto3 Docs 1.26.61 documentation to execute the below update_item & conditionexpression
            #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item

            try: 
                client=boto3.resource('dynamodb', region_name='us-east-1')
                table=client.Table("jchen201_annotations")
                table.update_item(
                    Key={'job_id':job_id},
                    UpdateExpression='SET job_status = :val1, s3_results_bucket = :val2, s3_key_result_file = :val3, s3_key_log_file = :val4, complete_time = :val5', 
                    ConditionExpression='contains(job_status, :val6)',
                    ExpressionAttributeValues={
                        ':val1':'COMPLETED',
                        ':val2': s3_results_bucket,
                        ':val3': ann_key_name,
                        ':val4': log_key_name,
                        ':val5': complete_time,
                        ':val6': 'RUNNING'
                    })

            except ClientError as e:
                print(e)

            # # remove item from local path
            try: 
                os.remove(f'{file_path}.annot.vcf')
                os.remove(f'{file_path}.vcf.count.log')
                os.remove(f'{file_path}.vcf')

            except OSError as e:
                print(e)
            except Exception as e:
                print(e) 

    else:
        print("A valid .vcf file must be provided as input to this program.")

            


### EOF