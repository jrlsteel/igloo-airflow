REGION=eu-west-1
TAG=latest
ROLE=arn:aws:iam::$AWS_ACCOUNT_ID:role/ci-agent-role

echo "===== assuming permissions => $ROLE ====="
KST=(`aws sts assume-role --role-arn $ROLE --role-session-name "deployment-$TAG" --query '[Credentials.AccessKeyId,Credentials.SecretAccessKey,Credentials.SessionToken]' --output text`)
unset AWS_SECURITY_TOKEN
export AWS_DEFAULT_REGION=$REGION
export AWS_ACCESS_KEY_ID=${KST[0]}
export AWS_SECRET_ACCESS_KEY=${KST[1]}
export AWS_SESSION_TOKEN=${KST[2]}
export AWS_SECURITY_TOKEN=${KST[2]}

echo "===== Grabbing code artifact token ====="
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain igloo --domain-owner 555393537168 --query authorizationToken --output text`