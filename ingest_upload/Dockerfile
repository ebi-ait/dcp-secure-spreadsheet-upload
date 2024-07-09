# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Copy the requirements.txt file from the parent directory into the container
COPY requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt

# Install git and the required Python packages
RUN yum -y install git
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy the Lambda function code, common directory, and config.json from the current directory into the container
COPY ingest_upload/upload_to_ingest_lambda.py ${LAMBDA_TASK_ROOT}/
COPY ingest_upload/config.json ${LAMBDA_TASK_ROOT}/
COPY common ${LAMBDA_TASK_ROOT}/common

# Set the CMD to your Lambda function handler
CMD [ "upload_to_ingest_lambda.lambda_handler" ]
