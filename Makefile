# The commit hash of the current HEAD of the repo.
HEAD_COMMIT_HASH  := $(shell git rev-parse HEAD)

# Work out what version we are building, based on nearest tag to the commit we are building.
# If there are no tags yet, default the version to 0.0.1.
VERSION           := $(shell git describe --tags $(HEAD_COMMIT_HASH) 2> /dev/null || echo v0.0.0)
PYTHON            := python3

# The directory that contains the code to be deployed
SOURCE_PATH := $(shell pwd)

# The directory within the EFS filesystem in to which the code should be
# deployed
AIRFLOW_EFS_CODE_DEPLOY_PATH=opt.airflow
AIRFLOW_EFS_DAG_DEPLOY_PATH=usr.local.airflow.dags/igloo-cdw-airflow

AIRFLOW_EFS_CONFIG_DEPLOY_PATH=opt.airflow/cdw/conf/config.py

.PHONY: git-flow-init
.PHONY: install
.PHONY: install-test-deps
.PHONY: test
.PHONY: ci-test
.PHONY: version
.PHONY: build
.PHONY: deploy
.PHONY: release-finish

git-flow-init:
ifeq ($(CI),true)
    # Bitbucket only checks out $(BITBUCKET_BRANCH) so git flow init fails
	# because there are no develop/master branches available.
	# Make sure that we have the master and develop branches available
	# so that we can `git flow init`.
	git fetch origin "+refs/heads/*:refs/remotes/origin/*"
	git checkout -b develop origin/develop
	# Switch back to the original branch
	git checkout $(BITBUCKET_BRANCH)
endif
	# And finally, initialise git-flow with all defaults and v prefix for version tags
	git flow init -f -d -t v

# Run `make prepare` after cloning the repo to set up git hooks and build scripts.
prepare:
	npm install
	npm run-script prepare

install:
	pip3 install -r requirements.txt

install-test-deps:
	pip3 install -r requirements-dev.txt

check-code-formatting:
	# Run black in check mode, causing it to bail out with an error if any changes would be made.
	# This is intended to be used in CI pipelines, where we want the pipeline to fail if the
	# code formatting is not correct.
	black --check .

code-formatting:
	# Run black on everything
	black .

code-formatting-pre-commit:
	# Run black on all Python files that are currently staged, and then add 'git add' them all
	git diff -z --name-only --staged **/*.py | xargs -0 black
	git diff -z --name-only --staged **/*.py | xargs -0 git add

cdw/conf/config.py:
	cp test/config.py cdw/conf/

test: cdw/conf/config.py
	rm -rf .coverage coverage.xml htmlcov
	coverage run -m pytest --ignore=archive --ignore=dags
	coverage xml

version:
	@echo $(VERSION)

build:
	# TODO

# Deploys code to a location in the local file system.
# AIRFLOW_EFS_ROOT should be the location in the filesystem where the
# Airflow EFS filesystem is mounted.
deploy:
	@echo Copying config file to $(SOURCE_PATH)/cdw/conf
	cp $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_CONFIG_DEPLOY_PATH) $(SOURCE_PATH)/cdw/conf

	@echo Copying code to $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_CODE_DEPLOY_PATH)
	rsync \
		-va \
		--delete \
		$(SOURCE_PATH)/cdw $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_CODE_DEPLOY_PATH)

	@echo Copying DAGs to $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_DAG_DEPLOY_PATH)
	rsync \
		-va \
		--delete \
		$(SOURCE_PATH)/dags $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_DAG_DEPLOY_PATH)

	# Changing the ownership of the process_EPC folder such that airflow is able to write to it.
	# This is needed because the EPC ETL downloads a zip file that unpacks it into that directory.
	chown 1000.1000 $(AIRFLOW_EFS_ROOT)/$(AIRFLOW_EFS_CODE_DEPLOY_PATH)/cdw/process_EPC

release-finish:
	# Set GIT_MERGE_AUTOEDIT=no to avoid invoking the editor when merging
	# to master.
	GIT_MERGE_AUTOEDIT=no git flow release finish -p -m "$(DOCKER_IMAGE_NAME) $(DOCKER_IMAGE_TAG)"
