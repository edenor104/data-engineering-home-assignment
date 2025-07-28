#!/bin/bash
set -e

# Load environment variables
if [ -f .env ]; then
  source .env
fi

# Display loaded variables
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
# echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"  # Avoid printing secrets
echo "STACK_NAME: $STACK_NAME"
echo "AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"

# Export variables
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

printf '\nUpdating stack...\n\n'

stack_yml="stack.yml"
stack=$STACK_NAME
echo "Stack: $stack"

# Check if stack exists
stack_exists=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_DEFAULT_REGION" || echo -1)

if [ "$stack_exists" = "-1" ]; then
    echo "Creating a new stack: $stack"
    aws cloudformation create-stack --stack-name "$stack" \
        --region "$AWS_DEFAULT_REGION" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"

    echo "Waiting for stack creation to complete: $stack"
    aws cloudformation wait stack-create-complete --stack-name "$stack" --region "$AWS_DEFAULT_REGION"
    status=$?
else
    echo "Updating the stack: $stack"
    aws cloudformation update-stack --stack-name "$stack" \
        --region "$AWS_DEFAULT_REGION" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"

    echo "Waiting for stack update to complete: $stack"
    aws cloudformation wait stack-update-complete --stack-name "$stack" --region "$AWS_DEFAULT_REGION"
    status=$?
fi

if [[ $status -ne 0 ]]; then
    echo "$stack operation failed with AWS error code: $status."
    exit $status
else
    echo "$stack operation completed successfully."
fi
