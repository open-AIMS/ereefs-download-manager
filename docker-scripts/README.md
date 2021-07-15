This documentation describes how to manually compile the *Docker image*
and upload it to *AWS ECR* (*Amazon Elastic Container Registry*).

In the context of the AIMS eReefs platform, this procedure is automated by a `Jenkins` task. 
This manual process is described here for anyone attempting to create a similar infrastructure.

**NOTE**: For security reason, this document use the fictitious
*AWS Account number* `123456789012`.
You will need to replace it with your own *AWS Account number*.

## Docker
This project is deployed on *AWS ECR* infrastructure.

## Compile
Compile the *jar* file and build the *Docker image*.

1. `$ cd ~/path/to/project/ereefs-download-manager`
2. `$ docker-scripts/build-jar.sh`
3. `$ docker-scripts/build-docker-image.sh`

## Run Docker locally (optional)
Optionally test the *Docker image* locally.

**NOTE:** This script has NOT been adapted for this project.
The following command won't work!
It was kept in the project for reference only.

```$ ./docker-scripts/run-docker.sh```

## Initialisation
Prepare your local computer to communicate with the *AWS* infrastructure
and prepare the *AWS* infrastructure to receive the *Docker image*.

The following steps only needs to be done once.

### Create a AWS Access Key
1. Go to `AWS Services > Security, Identity, & Compliance > IAM`
2. Click on your username (example: `johndoe`)
3. Click on the `Security credentials` tab
4. Click `Create access key` button
5. Copy the credentials

### Install AWS Cli on your computer
You will need the AWS Cli to authenticate to *AWS*,
to push the *Docker image* to *AWS ECR*.

```
$ sudo apt-get install awscli
$ aws configure
    AWS Access Key ID [XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX]: (enter the key generated earlier)
    AWS Secret Access Key [AKIAXXXXXXXXXXXXXXXX]: (enter the secret generated earlier)
    Default region name [ap-southeast-2]:
    Default output format [json]:
```

Edit AWS configuration file and move credentials to another profile,
for security reason. This will prevent your credentials from been
accidentally used. They will only be used if you explicitly specify
your username in the command line.

For example: move credentials to *johndoe* profile.

```$ vim ~/.aws/credentials```

```
[default]
aws_secret_access_key =
aws_access_key_id =

[johndoe]
aws_secret_access_key = XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
aws_access_key_id = AKIAXXXXXXXXXXXXXXXX
```

### Create the Docker repository
Create a repository for your Docker image on *AWS ECR*.

1. Go to `AWS Services > Containers > Elastic Container Registry`
2. Click `Create repository`

In this documentation, we will be using the repository
`ereefs-download-manager` with URL
`123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/ereefs-download-manager`.

*AWS* gives a list of directives to follow in order to push your
docker container to the *AWS ECR* repository,
but you should not need them if you follow this guide.
If you still want to see the instructions, select the repository
and click the `View push commands` button.

## Authenticate to AWS using AWS Cli
You will need to be authenticated for the push command to work.
The authentication token last for 24 hours, so you should not need to
do this more than once a day.

**NOTE**: Change the profile name to the one you put in
`~/.aws/credentials` file.

```$ $(aws ecr get-login --profile johndoe --no-include-email --region ap-southeast-2)```

If you are using an older version of *AWS Cli*, you might
get the following error:  
`Unknown options: --no-include-email`

If it's the case, follow this simple workaround:
1. Get the login command by running `$ aws ecr get-login --profile johndoe --region ap-southeast-2`
2. Copy / paste the output, remove the `-e none` and execute the command

    Example:

    Output:
    ```
    $ docker login -u AWS -p JcjdAS...cjuQsd -e none https://123456789012.dkr.ecr.ap-southeast-2.amazonaws.com
    ```

    Command to run:
    ```
    $ docker login -u AWS -p JcjdAS...cjuQsd https://123456789012.dkr.ecr.ap-southeast-2.amazonaws.com
    ```

    Expected result:
    ```
    Login Succeeded
    ```

## Upload the Docker image to AWS ECR
```
$ cd ~/path/to/project/ereefs-download-manager
$ docker tag ereefs-download-manager:latest 123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/ereefs-download-manager:latest
$ docker push 123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/ereefs-download-manager:latest
```

### Delete old Docker images
The old images occupy spaces on ECR. The storage cost <a href="https://aws.amazon.com/ecr/pricing/">$0.10 per GB-month</a>.
It's good practice to delete them if we are not planning to reuse them.

1. Go to `AWS Services > Containers > Elastic Container Registry`
2. Click `ereefs-download-manager`
3. Select all the `untagged` images (select all using the checkbox on top then unselect the `latest` image)
4. Click the `Delete` button in the top right corner of the page


## Run the Docker image using AWS Batch

### Initialisation
Prepare the *AWS* infrastructure to be allowed to run the
`ereefs-download-manager` docker container.

#### Create a Job Role
This role will be used by the `ereefs-download-manager` docker container
to access some *AWS* services.

1. Go to `AWS Services > Security, Identity, & Compliance > IAM > Roles`
2. Click `Create Role`
3. Select type of trusted entity: `AWS service`
    - Choose the service that will use this role: Select `Elastic Container Service`
    - Select your use case: `Elastic Container Service Task`
    - Next: Permissions
4. Attached permissions policies
    - `AmazonS3FullAccess`
    - `AmazonSNSFullAccess`
    - Next: Tags
5. Next: Review
6. Role name: `eReefsDownloadManagerRole`
    - Role description: `Role used by the ereefs-download-manager`
    - Create Role

#### Create a Compute Environment
If there is no suitable `Compute Environment`, you will need to create
one on which the *Docker image* will run.

This documentation is using the `Compute Environment` named `ecs_ereefs_100G`.

#### Create a Job Definition
For the purpose of this guide, a job definition is the setting
which describe the minimum requirements for a `Docker image`.

1. Go to `AWS Services > Compute > Batch > Job definitions`
2. Click the `Create` button
3. Create a job definition
    - Job definition name: `ereefs-download-manager-jobdef`
    - Environment
        - Job role: `eReefsDownloadManagerRole`
        - Container image: `123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/ereefs-download-manager`
        - vCPUs: `1`
        - Memory (MiB): `3072`
    - Create Job Definition

#### Create a Job Queue
If there is no suitable `Job Queue`, you will need to create
one.

This documentation is using the `Job Queue` named `ereefs-large-disk-ondemand`.

1. Go to `AWS Services > Compute > Batch > Job Queues`
2. Click the `Create queue` button
    - Create a job queue
        - Queue name: `ereefs-large-disk-ondemand`
        - Priority: `1`
    - Connected compute environments for this queue
        - `ecs_ereefs_100G`
    - Create Job Queue
3. Select the new Job Queue

### Run the Job
1. Go to `AWS Services > Compute > Batch > Job definitions`
2. Click on `ereefs-download-manager-jobdef` to expand it
3. Select the latest revision (example: `Revision 3`) by clicking the radio button
4. Select `Actions > Submit job`
    - Submit an AWS Batch Job
        - Job run-time
            - Job name: `ereefs-download-manager-job_2019-02-27_11h56`
            - Job definition: `ereefs-download-manager-jobdef:3`
            - Job queue: `ereefs-large-disk-ondemand`
            - Job Type: `Single`
    - Submit job

#### Result
After creating a `Job`, AWS will attempt to create a suitable
server and run the docker container on it.

You will need to monitor the `Job` to be sure it's running as expected.

1. Go to `AWS Services > Compute > Batch > Jobs`
2. The job appear in Status `submitted`
3. The job status will automatically change to `pending`, `runnable`, `starting`, `running` then `succeeded` OR `failed`
4. Click the task to see the `Job details`, scroll down to `Attempts` and click the `View logs` link
    to see the logs outputted by the `Docker image`.

**NOTE**: If your task stays in `runnable` for several minutes, it's
    likely that the `Job Definition` define requirements that can't
    be satisfied by any available computer from the `Compute Environment`.
    You will need to either changes the `Job Definition`
    or the `Compute Environment`.
