// Jenkins Pipeline script executed when a change is detected in Github. This script runs the tests
// for all changes. If the change is to the 'production' branch, this script also packages the
// application, builds the Docker image, and deploys to AWS ECR.

pipeline {

    // Default agent is any free agent.
    agent any

    parameters {
        choice(name: 'deployTarget', choices: ['', 'Testing', 'Production'], description: 'Choose the AWS account to deploy to. If no account is selected, it will not be deployed to AWS. IMPORTANT: only the "production" branch can be deployed to the "Production" account.')
        string(name: 'executionEnvironment', defaultValue: 'test', description: 'Enter the environment to create. This is used as a suffix for all components, and should be either "test", "prod", or a developer name (eg: "asmith").')
    }

    environment {

        // The name of the application, specified here for consistency.
        APP_NAME = 'ereefs-download-manager'

        // Maven-related
        // -------------
        // The name Maven uses when packaging the Java app.
        JAR_NAME = "${APP_NAME}-jar-with-dependencies.jar"

        // AWS-related
        // -----------
        // Credential ID for deploying to AWS.
        AWS_CREDENTIALS_ID_PROD = "jenkins-ereefs-prod-download_manager"
        AWS_CREDENTIALS_ID_TEST = "jenkins-ereefs-test-download_manager"

        //  AWS CloudFormation Id for project.
        AWS_CLOUD_FORMATION_STACKNAME_PREFIX = "downloadManager"

        // The deployment target based on the users' selection.
        AWS_DEPLOY_TARGET = "${params.deployTarget == 'Production' ? 'prod' : 'testing'}"

        // S3 bucket for Lambda deployment.
        AWS_LAMBDA_S3_DEPLOY_BUCKET = "aims-ereefs-private-${params.executionEnvironment}"

        // Docker-related
        // --------------
        // The name of the Docker image that will be built to run the compiled app.
        IMAGE_NAME = "ereefs-download_manager-${params.executionEnvironment}"

        // AWS account ID depending on the target environment
        // Parameters for connecting to the AWS ECR (container repository).
        ECR_PROD_URL = "https://${EREEFS_AWS_PROD_ACCOUNT_ID}.dkr.ecr.${EREEFS_AWS_REGION}.amazonaws.com"
        ECR_TEST_URL = "https://${EREEFS_AWS_TEST_ACCOUNT_ID}.dkr.ecr.${EREEFS_AWS_REGION}.amazonaws.com"
        ECR_CREDENTIALS_PROD = "ecr:${EREEFS_AWS_REGION}:${AWS_CREDENTIALS_ID_PROD}"
        ECR_CREDENTIALS_TEST = "ecr:${EREEFS_AWS_REGION}:${AWS_CREDENTIALS_ID_TEST}"

        MAVEN_REPO = "/workspace/.m2_${params.deployTarget}/repository"

        // Retrieve the credentials from the Credentials Manager in Jenkins, for accessing Github Packages.
        GITHUB_PACKAGES_CREDENTIALS = credentials('github-packages')

        LAMBDA_MD5SUM = sh(script: "md5sum src/lambda/sns-listener/index.js | cut -d' ' -f1", returnStdout: true).trim()
    }

    stages {

        // NOTE: Use "mvn -Dmaven.repo.local=${MAVEN_REPO} ..." to avoid creating a new Maven repository for each project

        // Use Maven to build and test the app, archiving the results.
        stage('Maven tests') {

            when {
                anyOf {
                    expression {
                        return params.deployTarget == 'Production' && env.BRANCH_NAME == 'production'
                    }
                    expression {
                        return params.deployTarget == 'Testing'
                    }
                }
            }

            // Maven will be executed within it's Docker container.
            agent {
                docker {
                    image 'maven:3.6-alpine'
                    reuseNode true
                }
            }

            // Run the tests (-B for non-interactive).
            steps {
                sh '''
                    mvn -settings maven-settings.xml -DGITHUB_USERNAME=$GITHUB_PACKAGES_CREDENTIALS_USR -DGITHUB_TOKEN=$GITHUB_PACKAGES_CREDENTIALS_PSW -Dmaven.repo.local=${MAVEN_REPO} -B clean test
                '''
            }

            // Define the handlers for post-processing.
            post {

                // Always capture the test results.
                always {

                    // Always archive the test results.
                    junit 'target/surefire-reports/*.xml'

                }

            }

        }

        // Use Maven to build and package the app. The resulting JAR file is stashed for use in
        // the Docker build stage.
        stage('Maven package') {

            when {
                anyOf {
                    expression {
                        return params.deployTarget == 'Production' && env.BRANCH_NAME == 'production'
                    }
                    expression {
                        return params.deployTarget == 'Testing'
                    }
                }
            }

            // Maven will be executed within it's Docker container.
            agent {
                docker {
                    image 'maven:3.6-alpine'
                    reuseNode true
                }
            }

            // Compile and package the app.
            steps {
                sh '''
                    mvn -B -settings maven-settings.xml -DGITHUB_USERNAME=$GITHUB_PACKAGES_CREDENTIALS_USR -DGITHUB_TOKEN=$GITHUB_PACKAGES_CREDENTIALS_PSW -Dmaven.repo.local=${MAVEN_REPO} -DbuildId=${JENKINS_BUILD_ID} -DskipTests=true package
                '''
            }

        }

        // Build the Docker image.
        stage('Docker build') {

            when {
                anyOf {
                    expression {
                        return params.deployTarget == 'Production' && env.BRANCH_NAME == 'production'
                    }
                    expression {
                        return params.deployTarget == 'Testing'
                    }
                }
            }

            steps {

                script {

                    // Build the Docker image.
                    docker.build(IMAGE_NAME, "--build-arg JAR_NAME=${JAR_NAME} --force-rm .")

                }

            }
        }


        // Deploy the Docker image and update the CloudFormation Stack.
        stage('Deploy to AWS "TEST" environment') {

            when {
                anyOf {
                    expression {
                        return params.deployTarget == 'Testing'
                    }
                }
            }

            steps {

                script {
                    // Make a Lambda deployment package.
                    zip zipFile: 'target/lambda/sns-listener-deploy.zip', dir: 'src/lambda/sns-listener'

                    // Generate the MD5 hash of the zip file
                    def md5sum = sh(script: "md5sum target/lambda/sns-listener-deploy.zip | cut -d' ' -f1", returnStdout: true).trim()

                    // Update the CloudFormation Stack.
                    withAWS(region: EREEFS_AWS_REGION, credentials: AWS_CREDENTIALS_ID_TEST) {
                        s3Upload(
                            bucket: "${AWS_LAMBDA_S3_DEPLOY_BUCKET}",
                            path: "deploy/download-manager/lambda/sns-listener-deploy-${md5sum}.zip", // Use the MD5 hash in the file name
                            file: "target/lambda/sns-listener-deploy.zip"
                        )
                        cfnUpdate(
                            stack: "${AWS_CLOUD_FORMATION_STACKNAME_PREFIX}-${params.executionEnvironment}",
                            params: ["Environment=${params.executionEnvironment}", "EcrUserId=${AWS_CREDENTIALS_ID_TEST}", "LambdaMd5sum=${md5sum}"],
                            tags: ["deployTarget=${params.deployTarget}","executionEnvironment=${params.executionEnvironment}"],
                            file: 'cloudformation.yaml',
                            timeoutInMinutes: 10,
                            pollInterval: 5000
                        )
                    }

                    // Credentials for connecting to the AWS ECR repository.
                    docker.withRegistry(ECR_TEST_URL, ECR_CREDENTIALS_TEST) {

                        // Deploy the Docker image.
                        docker.image(IMAGE_NAME).push(BUILD_NUMBER)
                        docker.image(IMAGE_NAME).push("latest")
                    }

                }
            }
        }


        // Deploy the Docker image and update the CloudFormation Stack.
        stage('Deploy to AWS "PRODUCTION" environment') {

            when {
                anyOf {
                    expression {
                        return params.deployTarget == 'Production' && env.BRANCH_NAME == 'production'
                    }
                }
            }

            steps {

                script {

                    // Make a Lambda deployment package.
                    zip zipFile: 'target/lambda/sns-listener-deploy.zip', dir: 'src/lambda/sns-listener'

                    // Update the CloudFormation Stack.
                    withAWS(region: EREEFS_AWS_REGION, credentials: AWS_CREDENTIALS_ID_PROD) {
                        s3Upload(
                            bucket: "${AWS_LAMBDA_S3_DEPLOY_BUCKET}",
                            path: "deploy/download-manager/lambda/sns-listener-deploy.zip",
                            file: "target/lambda/sns-listener-deploy.zip"
                        )
                        cfnUpdate(
                             stack: "${AWS_CLOUD_FORMATION_STACKNAME_PREFIX}-${params.executionEnvironment}",
                             params: ["Environment=${params.executionEnvironment}", "EcrUserId=${AWS_CREDENTIALS_ID_PROD}"],
                             tags: ["deployTarget=${params.deployTarget}","executionEnvironment=${params.executionEnvironment}"],
                             file: 'cloudformation.yaml',
                             timeoutInMinutes: 10,
                             pollInterval: 5000
                        )
                    }

                    // Credentials for connecting to the AWS ECR repository.
                    docker.withRegistry(ECR_PROD_URL, ECR_CREDENTIALS_PROD) {

                        // Deploy the Docker image.
                        docker.image(IMAGE_NAME).push(BUILD_NUMBER)
                        docker.image(IMAGE_NAME).push("latest")
                    }

                }
            }
        }
    }

    // Post-processing.
    post {

        cleanup {

            sh'''

                # Remove any Docker containers that are not in use.
                docker container prune --force

                # Remove any Docker images that are not in use.
                docker image prune --force

                # Remote any Docker networks that are not in use.
                docker network prune --force

            '''
        }

    }

}
