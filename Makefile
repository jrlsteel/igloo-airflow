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
.PHONY: test
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

test:
	# TODO: Implement tests

version:
	@echo $(VERSION)

build:
	# TODO

$(PACKAGE_NAME)-$(VERSION).zip:
	git archive --format=zip -o enzek-meterpoint-readings-${VERSION}.zip HEAD

package: $(PACKAGE_NAME)-$(VERSION).zip

deploy: package
	aws s3 cp $(PACKAGE_NAME)-$(VERSION).zip s3://$(AWS_S3_BUCKET)

release-finish:
	# Set GIT_MERGE_AUTOEDIT=no to avoid invoking the editor when merging
	# to master.
	GIT_MERGE_AUTOEDIT=no git flow release finish -p -m "$(DOCKER_IMAGE_NAME) $(DOCKER_IMAGE_TAG)"
