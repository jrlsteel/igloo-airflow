AWS_ACCOUNT_ID_UAT=555393537168

# If CI=true then we always expect the AWS_ACCOUNT_ID and ENVIRONMENT
# variables to be set, however if we are running outside of CI (e.g.
# locally in a terminal) then we provide some sensible defaults.
ifneq ($(CI),true)
# AWS_ACCOUNT_ID should be passed on the command line, e.g.
# make -e AWS_ACCOUNT_ID=1234
AWS_ACCOUNT_ID ?= $(AWS_ACCOUNT_ID_UAT)

# The ENVIRONMENT variable must be set to one of the allowed values for the
# Environment parameter of the SAM template.
ENVIRONMENT ?= uat
endif

# The commit hash of the current HEAD of the repo.
HEAD_COMMIT_HASH  := $(shell git rev-parse HEAD)

# Work out what version we are building, based on nearest tag to the commit we are building.
# If there are no tags yet, default the version to 0.0.1.
VERSION           := $(shell git describe --tags $(HEAD_COMMIT_HASH) 2> /dev/null || echo v0.0.0)

# Role that will be assumed prior to deploying
CI_AGENT_ROLE_ARN      := arn:aws:iam::${AWS_ACCOUNT_ID}:role/ci-agent-role

# Role that will be passed to SAM to when deploying
CI_DEPLOYMENT_ROLE_ARN := arn:aws:iam::$(AWS_ACCOUNT_ID):role/ci-deployment-role

SAM_STACK_NAME    := enzek-meterpoint-readings

# If we are running in CI (Bitbucket) then there is no need to set the
# --use-container argument as we should be running inside an appropriate
# container anyway.

ifeq ($(CI),true)
SAM_BUILD_OPTS=--parameter-overrides Environment=$(ENVIRONMENT)
else
SAM_BUILD_OPTS=--use-container --parameter-overrides Environment=$(ENVIRONMENT)
endif

SAM_DEPLOY_OPTS=--stack-name $(SAM_STACK_NAME) \
--s3-bucket ci-artifacts-$(AWS_ACCOUNT_ID)-eu-west-1 \
--capabilities CAPABILITY_NAMED_IAM \
--region eu-west-1 \
--role-arn $(CI_DEPLOYMENT_ROLE_ARN) \
--no-fail-on-empty-changeset \
--parameter-overrides Environment=$(ENVIRONMENT)

.PHONY: git-flow-init
.PHONY: test
.PHONY: version
.PHONY: build
.PHONY: deploy
.PHONY: release-finish
.PHONY: test-functions-deploy

git-flow-init:
ifeq ($(CI),true)
	# When a pipeline is executed, bitbucket only pulls down the specific
	# branch that is needed for the current pipeline execution. `git flow init`
	# tends to fail if the develop branch is not available, so we have to do
	# some messing around to make sure it's here before we execute `git flow init`.
	git fetch origin "+refs/heads/*:refs/remotes/origin/*"
	git checkout -b develop origin/develop
	# Switch back to the original branch
	git checkout $(BITBUCKET_BRANCH)
endif
	# And finally, initialise git-flow with all defaults and v prefix for version tags
	git flow init -f -d -t v

test: test-functions-deploy

version:
	@echo $(VERSION)

build: sam-build

deploy: sam-deploy

release-finish:
	# Set GIT_MERGE_AUTOEDIT=no to avoid invoking the editor when merging
	# to master.
	GIT_MERGE_AUTOEDIT=no git flow release finish -p -m "$(BITBUCKET_REPO_FULL_NAME) $(VERSION)"

.PHONY: sam-build
.PHONY: sam-deploy

# Any files (like custom_packages or config files) that need to be copied
# in to the individual lambda function directories should be added to the
# list of dependencies after `sam-build: ` and a recipe should be added at
# the bottom of this file to actually copy them in.

sam-build:
	sam build $(SAM_BUILD_OPTS)

sam-deploy:
	# We need to assume the ci-agent-role role before calling cloudformation
	KST=`aws sts assume-role --role-arn $(CI_AGENT_ROLE_ARN) --role-session-name $(SAM_STACK_NAME)-deployment-session --query '[Credentials.AccessKeyId,Credentials.SecretAccessKey,Credentials.SessionToken]' --output text` ; \
	export AWS_DEFAULT_REGION=eu-west-1 ; \
	export AWS_ACCESS_KEY_ID=`echo $${KST} | awk '{print $$1}'` ; \
	export AWS_SECRET_ACCESS_KEY=`echo $${KST} | awk '{print $$2}'` ; \
	export AWS_SESSION_TOKEN=`echo $${KST} | awk '{print $$3}'` ; \
	export AWS_SECURITY_TOKEN=`echo $${KST} | awk '{print $$3}'` ; \
	sam deploy $(SAM_DEPLOY_OPTS)

test-functions-deploy:
	echo "TODO: test-functions-deploy"