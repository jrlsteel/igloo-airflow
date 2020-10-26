PACKAGE_NAME      := enzek-meterpoint-readings
# The commit hash of the current HEAD of the repo.
HEAD_COMMIT_HASH  := $(shell git rev-parse HEAD)

# Work out what version we are building, based on nearest tag to the commit we are building.
# If there are no tags yet, default the version to 0.0.1.
VERSION           := $(shell git describe --tags $(HEAD_COMMIT_HASH) 2> /dev/null || echo v0.0.0)
PYTHON            := python3
PACKAGE_INCLUDE   := * .venv
PACKAGE_EXCLUDE   := .git .gitignore .venv $(PACKAGE_NAME)-*.zip
AWS_S3_BUCKET     := $(PACKAGE_NAME)-artifacts-${ENVIRONMENT}-$(AWS_ACCOUNT_ID)

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

install:
	pip3 install -r requirements.txt

install-test-deps:
	pip3 install -r requirements-dev.txt

conf/config.py:
	mkdir -p conf/
	cp test/config.py conf/

test: conf/config.py
	cd process_WeatherData && python3 -m unittest test_processHourlyWeatherData.py

ci-test: install-test-deps test

version:
	@echo $(VERSION)

