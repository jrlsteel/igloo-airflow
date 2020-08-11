enzek-meterpoint-readings
=========================

This repo contains all functionality required by the various batch processes that run on a daily / weekly basis to extract data from various sources and ingest it in to the Customer Data Warehouse (CDW).

## Development Guide

When code is committed to this repo and pushed to Bitbucket a CI pipeline will be triggered. For various branches, these commits will initiate a deployment of the code to a corresponding AWS environment.

The branch to environment mapping is as follows:

| Branch    | Deployment Environment |
| ----------|------------------------|
| develop   | UAT                    |
| release/* | Preprod                |
| master    | Prod                   |

### Git Flow

It is highly recommended to install the git-flow extensions. Once this is done, the following should be executed after cloning this repo:

```
make git-flow-init
```

### Feature Development

All new feature development should take place on a feature branch. The branch name should be prefixed with `feature/` and should generally start with the Jira ticket number, e.g. `feature/DMRE-1234-a-short-text-description-if-desired`. The feature branch should generally use `develop#HEAD` as it's start point.

A suitable branch can be created with the following commands:

```
git checkout develop
git checkout -b feature/DMRE-1234-a-short-text-description-if-desired
```

If the git-flow extensions are available, the following command will create the same branch:

```
git flow feature start DMRE-1234-a-short-text-description-if-desired
```

When development of the feature is complete, the branch should be pushed to Bitbucket and a Pull Request (PR) created:

```
git push --set-upstream origin feature/DMRE-1234-a-short-text-description-if-desired
```

If the git-flow extensions are available, use the following command:

```
git flow feature publish
```

When the Pull Request is merged to the `develop` branch a CI pipeline will automaticaly deploy the changes to the AWS UAT environment.

### Release Process

The release process involves creating a new release branch and pushing it to Bitbucket. This will trigger a CI pipeline that deploys the changes to the AWS Preprod environment.

A release branch can be created and pushed to Bitbucket with the following commands:

```
git checkout develop
git checkout -b release/1.2.3
git push --set-upstream origin release/1.2.3
```

If the git-flow extensions are available, the following command will create the same branch:

```
git flow release create 1.2.3
git flow release publish
```

Any required changes can now be made committed to the release branch and published to Bitbucket. Whenever the release branch is published, it will be automatically deployed to the AWS Preprod environment.

When all testing has been completed, the release can be 'finished'. This will ultimately lead to the `master` branch being updated and deployed to the AWS Production environment.

#### Deploying to Production

Every time the Bitbucket pipeline is executed for a release branch, a manual step is available to run a `Finish Release` process. When the release branch is ready to be deployed to Production, this `Finish Release` process should be executed by clicking the `Run` button in the pipeline.

This will trigger a merge of the release branch to master, which will result in a further pipeline being executed.

This pipeline can be found here:

https://bitbucket.org/iglooenergy/igloo-datawarehouse-api/addon/pipelines/home#!/results/branch/master/

You should see a further manual step in this pipeline `Deploy to Production`. When this is executed the deployment will commence.
