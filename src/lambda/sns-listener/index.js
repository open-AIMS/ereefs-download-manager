/**
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */

/*  **Disclaimer of Liability**
 *  USE AT YOUR OWN RISK
 *  In no event will AIMS be liable for any incidental, special, or
 *  consequential damages resulting from this script.
 *
 *
 *  Name: Download manager SNS listener (ereefs-download-manager-sns-listener)
 *  Language: Node.js
 *  SNS Event message structure: JSON Object
 *      {
 *          limit (Integer): Define the maximum number of files to download in each download definition. Negative number to download all available files. Default: 2.
 *          dryRun (Boolean): True to test the script (no files are downloaded).
 *          downloadDefinitionId (String): The download definition to download. Default: All enabled download definitions.
 *      }
 *
 *  Developer documentation:
 *      API Doc: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Batch.html
 *      Example: https://github.com/developmentseed/aws-batch-example/blob/master/lambda/index.js
 */

// Load the SDK for JavaScript
const AWS = require('aws-sdk');
// Set the region
AWS.config.update({region: 'ap-southeast-2'});

/**
 * Structure of the SNS event object:
 *     {
 *         "Records": [
 *             {
 *                 "EventSource": "aws:sns",
 *                 "EventVersion": "1.0",
 *                 "EventSubscriptionArn": "arn:aws:sns:<REGION ID>:<ACCOUNT ID>:download-request:<GUID>",
 *                 "Sns": {
 *                     "Type": "Notification",
 *                     "MessageId": "<GUID>",
 *                     "TopicArn": "arn:aws:sns:<REGION ID>:<ACCOUNT ID>:download-request",
 *                     "Subject": "<String>",
 *                     "Message": "<Stringified JSON message>",
 *                     "Timestamp": "<Date>",
 *                     "SignatureVersion": "1",
 *                     "Signature": "<Large base 64 string>",
 *                     "SigningCertUrl": "https://sns.<REGION ID>.amazonaws.com/SimpleNotificationService-<ID>.pem",
 *                     "UnsubscribeUrl": "https://sns.<REGION ID>.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:<REGION ID>:<ACCOUNT ID>:download-request:<GUID>",
 *                     "MessageAttributes": {}
 *                 }
 *             }
 *         ]
 *     }
 *
 * NOTE: The only attribute this Lambda function needs is the "Message"
 * Example (used in Lambda function "test events"):
 * {
 *   "Records": [
 *     {
 *       "Sns": {
 *         "Message": "{ \"limit\": -1, \"dryRun\": true, \"downloadDefinitionId\": \"downloads__ereefs__gbr4_v2_daily\" }"
 *       }
 *     }
 *   ]
 * }
 *
 * Structure of the "Stringified JSON message":
 *     {
 *         limit {integer}: Maximum number of files to download in each download definition. Negative number to download all available files. Default: 2.
 *         dryRun {boolean}: True to test the script (no write / delete).
 *         downloadDefinitionId {string}: The download definition to download. Default: All enabled download definitions.
 *     }
 */

exports.handler = function (event, context, callback) {
  try {
    // Get SNS Message
    // See: https://docs.aws.amazon.com/lambda/latest/dg/with-sns-example.html
    const snsTextMessage = event.Records[0].Sns.Message;

    // The message text can be anything.
    // It seems that the common approach is to send a strignified JSON Object.
    const snsJSONMessage = JSON.parse(snsTextMessage);

    const limit = parseInt(snsJSONMessage.limit) || 2;

    // dryRun is true unless specified otherwise (safety feature)
    const dryRun = snsJSONMessage.dryRun !== false && snsJSONMessage.dryRun !== "false";

    const downloadDefinitionId = snsJSONMessage.downloadDefinitionId;

    const files = snsJSONMessage.files;

    const batch = new AWS.Batch();

    console.log("Checking if DownloadManager is already running.");

    // Start the processing chain by retrieving the list of all Jobs for JobQueue that
    // have a status of: SUBMITTED, PENDING, RUNNABLE, STARTING or RUNNING. Each of
    // these states must be queried separately. They are wrapped in individual
    // promises, which are then wrapped in a Promise.all to wait for all individual
    // promises to resolve. The result will be an array of JobId value for all Jobs in
    // any of these states, or an empty array if not Jobs found.
    const STATUS_LIST = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"];
    const listJobsPromises = [];
    STATUS_LIST.forEach(function (status) {
      console.log('status: ' + status);
      listJobsPromises.push(
        new Promise(function (resolve, reject) {
          const params = {
            jobQueue: process.env.JOB_QUEUE_ARN,
            jobStatus: status
          };
          batch.listJobs(params, function (err, data) {
            if (err) {
              reject(err);
            } else {
              resolve(data);
            }
          });
        })
      );
    });

    // Wait for all queries to resolve (respond).
    Promise.all(listJobsPromises)
      .then(function (responses) {

        // Build a list of JobIds from the responses.
        const jobIds = [];
        console.log('responses: ' + JSON.stringify(responses));
        responses.forEach(function (response) {
          console.log('response: ' + JSON.stringify(response));
          if (response.jobSummaryList && response.jobSummaryList.length > 0) {
            response.jobSummaryList.forEach(function (jobSummary) {
              console.log('jobSummary: ' + JSON.stringify(jobSummary));
              jobIds.push(jobSummary.jobId);
            });
          }
        });

        // Pass the list of JobIds to the next processor.
        return jobIds;
      })

      // Process the array of JobIds for the target JobQueue.
      .then(function (jobIds) {
        console.log('jobIds: ' + JSON.stringify(jobIds));

        // Invoke the DescribeJob API to identify the JobDefinition of each running
        // Job. This API call allows queries to be batched in groups of 100 JobIds.
        // Each batch will be handled by a single Promise, and all Promises will be
        // wrapped by a Promise.all to wait until all promises have reesolved.
        let batchDescribeJobPromises = [];

        // Group in batches of 100.
        const stepSize = 100;
        for (let step = 0; step < jobIds.length / stepSize; step++) {

          // Accumulate the list of JobIds in this batch.
          let batchJobIds = [];
          for (let count = 0; ((count < stepSize) && (step * stepSize + count < jobIds.length)); count++) {
            batchJobIds.push(jobIds[step * stepSize + count]);
          }

          // Add a Promise for this batch.
          batchDescribeJobPromises.push(
            new Promise(function (resolve, reject) {
              console.log('batchJobIds: ' + JSON.stringify(batchJobIds));
              batch.describeJobs({jobs: batchJobIds}, function (err, data) {
                if (err) {
                  reject(err);
                } else {

                  // API call returned successfullly, so check
                  // if any of the JobDefinitions match.
                  console.log('describeJob: ' + JSON.stringify(data));
                  if (data.jobs && data.jobs.length > 0) {
                    resolve(
                      data.jobs.some(function (job) {
                        return job.jobDefinition === process.env.JOB_DEFINITION_ARN;
                      })
                    );
                  } else {
                    // No data returned.
                    resolve(false);
                  }
                }
              });
            })
          );
        }

        // Wrap each the promises in a Promise.all to wait until all promises have
        // resolved, and then determine if any returned 'true'.
        return Promise.all(batchDescribeJobPromises)
          .then(function (values) {
            console.log('values: ' + JSON.stringify(values));
            // Transform the result from the Promise.all, which should be an
            // array of boolean results, one result from each batch promise.
            // Any value of 'true' indicates that a DownloadManager is running.
            return values.includes(true);
          });
      })

      // Respond to the boolean 'isRunning' as appropriate.
      .then(function (isRunning) {
        console.log('isRunning: ' + isRunning);
        if (!isRunning) {
          console.log("Starting DownloadManager");
          console.log("Event details:");
          console.log("    snsJSONMessage: " + JSON.stringify(snsJSONMessage, null, 2));
          console.log("    limit: " + limit);
          console.log("    dryRun: " + dryRun);
          console.log("    downloadDefinitionId: " + downloadDefinitionId);
          console.log("    files: " + files);
          const todayDateStr = getFormattedDate(new Date());

          // Submit a Job to AWS
          // API Doc: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Batch.html#submitJob-property
          const params = {
            'jobDefinition': process.env.JOB_DEFINITION_ARN,
            'jobQueue': process.env.JOB_QUEUE_ARN,
            'jobName': "ereefs-download_manager_" + todayDateStr,
            'containerOverrides': {
              'environment': [
                {'name': 'LIMIT', 'value': '' + limit},
                {'name': 'DRYRUN', 'value': dryRun ? 'true' : 'false'},
                {'name': 'DOWNLOADDEFINITIONID', 'value': downloadDefinitionId },
                {'name': 'FILES', 'value': files },
              ]
            }
          };
          const batch = new AWS.Batch();
          batch.submitJob(params, function (err, data) {
            if (err) {
              console.error("AWS Batch Error: " + err.message);
              context.fail(err);
              callback(err);
            } else {
              context.succeed();
              callback(null);
            }
          });
        } else {
          console.log('DownloadManager is already running.');
          context.succeed();
          callback(null);
        }
      })

      // Handle any errors that have occured within the Promise chain.
      .catch(function (error) {
        callback(error);
      });

    // Handle any exceptions that have been thrown.
  } catch (error) {
    console.error("Error: " + error.message);
    context.fail(error);
    callback(error);
  }
};

function getFormattedDate(date) {
  // ISO date: 2011-10-05T14:48:00.000Z
  // ":" and "." are invalid characters (for a AWS Batch Job Name)
  // Returns 2011-10-05T14h48_00-000Z
  return date.toISOString().replace(":", "h").replace(":", "_").replace(".", "-");
}
