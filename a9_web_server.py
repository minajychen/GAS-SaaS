from flask import Flask, redirect, url_for, render_template, request, jsonify
import subprocess, uuid, boto3, botocore, requests
from botocore.client import Config
from botocore.exceptions import ClientError
import time, json

app = Flask(__name__)


@app.route('/', methods=['GET'])
# This method is called when a user visits "/" on your server
def home():
  # substitute your favorite geek message below!
     return "Mina is here."

@app.route('/hello', methods=['GET'])
# This method is called when a user visits "/hello" on your server
def hello():
  # Another geeky message here!
    return "Cookies are great."


@app.route('/annotate', methods=['GET'])
def annotate():
    #I referenced the A6 write-up and the "Session Configurations" section to configure my S3 session.client below
    #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html
    session = boto3.session.Session()
    s3 = session.client("s3", config=Config(signature_version="s3v4"))

    bucket = 'gas-inputs'

    key = "jchen201" + "/" + "userx" + "/" + str(uuid.uuid4()) + "~" + "${filename}"

    #I consulted TA Jiamao and referenced the POST policy examples in AWS S3's POST policy to assign the below fields and conditions dictionaries
    #https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html


    #Dynamically retrieve the URL of the original request from the Flask request object & changing the route"    
    # redirect_url='http://jchen201-a9-web.ucmpcs.org:5000/annotate/job'

    redirect_url=request.url + '/job'

    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": "AES256",
        "acl": "private",
    }

    conditions = [
        ["eq", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": "AES256"},
        {"acl": "private"},
    ]

    try:
        #I consulted TA Jiamao and referenced the "S3.Client.generate_presigned_post" section of the Boto3 Docs 1.26.57 documentation to execute the below generate_presigned_post
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post
        presigned_post = s3.generate_presigned_post(
                Bucket=bucket,
                Key=key,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60)

    except ClientError as e:
        response={'code':500, 'status': 'error', 'message': 'Error occured while connecting to S3. {}'.format(e)}
        return jsonify(response), 500, {'Content-Type': 'application/json'}


    return render_template("annotate.html", presigned_post=presigned_post)

@app.route("/annotate/job", methods=['GET'])
def annotate_job():

    # Get bucket name, key, and job ID from the S3 redirect URL
    s3_key=request.args.get('key')
    s3_bucket=request.args.get('bucket')
    s3_key_st=str(s3_key)
    s3_bucket_st=str(s3_bucket)
    key_without_prefix=s3_key_st.split("/",2)[2]
    job_id=key_without_prefix.split("~",1)[0]
    input_file = key_without_prefix.split("~",1)[1]
    user_id=s3_key_st.split("/",2)[1]
    s3_results_bucket="gas-results"
    submit_time=round(time.time())

    #set Region
    REGION='us-east-1'

    # Create a job item and persist it to the annotations database. 
    data = {"job_id": job_id, 
           "user_id": user_id, 
           "input_file_name": input_file,
           "s3_inputs_bucket": s3_bucket_st,
           "s3_key_input_file": s3_key_st,
           "submit_time": submit_time,
           "job_status": "PENDING"}

    #I used this stackoverflow thread to understand how to put an item to a dynamodb table
    #https://stackoverflow.com/questions/33535613/how-to-put-an-item-in-aws-dynamodb-using-aws-lambda-with-python
    try: 
        client=boto3.resource('dynamodb', region_name=REGION)
        table=client.Table("jchen201_annotations")
        table.put_item(Item=data)
    except ClientError as e:
        response={'code':500, 'status': 'error', 'message': 'Error occured while putting an item to dynamodb. {}'.format(e)}
        return jsonify(response), 500, {'Content-Type': 'application/json'}

    # Set up SNS client
    try:
        #I referenced 'Working with Amazon SNS with Boto3' from Towards Data Science to execute the below SNS Publish message
        #https://towardsdatascience.com/working-with-amazon-sns-with-boto3-7acb1347622d

        sns=boto3.client('sns', region_name=REGION)
        topic_arn='arn:aws:sns:us-east-1:127134666975:jchen201_job_requests'
        response=sns.publish(TopicArn=topic_arn,
            Message=json.dumps(data),
            Subject=job_id)
        message_id=response['MessageId']
        print(message_id)

    except ClientError as e:
        response={'code':500, 'status': 'error', 'message': 'Error occured publishing a message to topic. {}'.format(e)}
        return jsonify(response), 500, {'Content-Type': 'application/json'}

    return jsonify({'code': 201, 'data': {'job_id': job_id, 'input_file': input_file}}), 201, {'Content-Type': 'application/json'}




# Run the app server
app.run(host='0.0.0.0', debug=True)