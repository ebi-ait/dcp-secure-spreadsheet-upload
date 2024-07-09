#!/bin/bash

set -e

# Define Lambda function name
FUNCTION_NAME_NOTIFY="notify-spreadsheet-upload-function"

# Function to package and deploy the notify_upload lambda function
package_and_deploy() {
    LAMBDA_DIR=$1
    FUNCTION_NAME=$2

    # Create a temporary directory for the package
    TEMP_DIR=$(mktemp -d)

    cp -r $LAMBDA_DIR/* $TEMP_DIR/
    cp -r common $TEMP_DIR/

    # Install dependencies from the root requirements.txt into the temporary directory
    echo "Installing dependencies..."
    pip install -r requirements.txt -t $TEMP_DIR/

    # Zip the package
    DEPLOY_PACKAGE="deploy_package.zip"
    echo "Creating zip package..."
    cd $TEMP_DIR
    zip -r9 $OLDPWD/$DEPLOY_PACKAGE .
    cd $OLDPWD

    if [ ! -f $DEPLOY_PACKAGE ]; then
        echo "Error: $DEPLOY_PACKAGE not found!"
        exit 1
    fi
    echo "$DEPLOY_PACKAGE created successfully."

    # Deploy to AWS Lambda
    echo "Deploying to AWS Lambda..."
    aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://$DEPLOY_PACKAGE

    # Clean up
    echo "Cleaning up..."
    rm -rf $TEMP_DIR
    rm -f $DEPLOY_PACKAGE

    echo "Deployment completed successfully."
}

# Deploy notify_upload Lambda function
package_and_deploy "notify_upload" $FUNCTION_NAME_NOTIFY
