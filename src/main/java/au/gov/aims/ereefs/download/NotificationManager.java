/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.Utils;
import au.gov.aims.ereefs.bean.download.DownloadBean;
import au.gov.aims.ereefs.bean.metadata.netcdf.NetCDFMetadataBean;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

// Java example:
//   https://docs.aws.amazon.com/sns/latest/dg/using-awssdkjava.html
public class NotificationManager implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(NotificationManager.class);

    // The Download Manager publish a message to this topic after every file downloaded
    // See cloudformation.yaml for actual value
    private static final String DOWNLOAD_COMPLETE_TOPIC_ARN_KEY = "DOWNLOAD_COMPLETE_TOPIC_ARN";

    // The Download Manager publish a message to this topic before exiting (only when files were downloaded)
    // See cloudformation.yaml for actual value
    private static final String ALL_DOWNLOAD_COMPLETE_TOPIC_ARN_KEY = "ALL_DOWNLOAD_COMPLETE_TOPIC_ARN";

    // The Download Manager publish a message to this topic when there is not enough disk space left or when a downloaded file is corrupted
    // See cloudformation.yaml for actual value
    private static final String ADMINISTRATION_TOPIC_ARN_KEY = "ADMINISTRATION_TOPIC_ARN";


    private AmazonSNS snsClient;

    public NotificationManager() {
        this.snsClient = AmazonSNSClientBuilder.defaultClient();
    }

    @Override
    public void close() throws Exception {
        this.snsClient.shutdown();
    }

    /**
     * Used to send a human readable SNS when a NetCDF file could not be downloaded
     * because there is not enough disk space left.
     * NOTE: The message is sent to the administration topic, which forward it by email.
     * @param fileUri The URI of the original NetCDF file.
     * @param fileSizeMB The file size, in MB.
     * @param freeSpaceMB The space left on disk, in MB.
     * @return The SNS message ID
     * @throws IOException
     */
    public String sendDiskFullNotification(URI fileUri, double fileSizeMB, double freeSpaceMB) throws IOException {
        String topicArn = System.getenv(ADMINISTRATION_TOPIC_ARN_KEY);

        String errorMessage = String.format(
                "ERROR: Disk Full%n" +
                "%n" +
                "Not enough disk space left on device to download the file %s.%n" +
                "File size: %.1f MB%n" +
                "Disk space left: %.1f MB",
                fileUri, fileSizeMB, freeSpaceMB);

        LOGGER.debug(String.format("Sending disk full SNS message: %s", errorMessage));
        return this.sendNotification(errorMessage, topicArn);
    }

    /**
     * Used to send a human readable SNS when a downloaded NetCDF file is corrupted.
     * NOTE: The message is sent to the administration topic, which forward it by email.
     * @param fileUri The URI of the original NetCDF file.
     * @param errorMessage The error message returned while attempting to extract the NetCDF file metadata.
     * @return The SNS message ID
     * @throws IOException
     */
    public String sendCorruptedFileNotification(URI fileUri, String errorMessage) throws IOException {
        return this.sendCorruptedFileNotification(fileUri, errorMessage, null);
    }

    public String sendCorruptedFileNotification(URI fileUri, String errorMessage, Exception ex) throws IOException {
        String topicArn = System.getenv(ADMINISTRATION_TOPIC_ARN_KEY);

        StringBuilder notificationMessageSb = new StringBuilder(String.format(
                "ERROR: Corrupted File%n" +
                "%n" +
                "The downloaded NetCDF file %s is corrupted:%n" +
                "%s",
                fileUri, errorMessage));

        if (ex != null) {
            Throwable cause = ex.getCause();
            while (cause != null) {
                notificationMessageSb.append(
                        String.format("%nCaused by: %s", Utils.getExceptionMessage(cause)));
                cause = cause.getCause();
            }
        }

        String notificationMessage = notificationMessageSb.toString();

        LOGGER.debug(String.format("Sending corrupted file SNS message: %s", notificationMessage));
        return this.sendNotification(notificationMessage, topicArn);
    }

    /**
     * Used to send a JSON SNS when all NetCDF files have been downloaded.
     * @param downloadOutputMap
     * @return The SNS message ID
     * @throws IOException
     */
    public String sendFinalDownloadNotification(Map<DownloadBean, NetCDFDownloadOutput> downloadOutputMap) throws IOException {
        if (downloadOutputMap == null || downloadOutputMap.isEmpty()) {
            return null;
        }

        String topicArn = System.getenv(ALL_DOWNLOAD_COMPLETE_TOPIC_ARN_KEY);

        JSONObject jsonMessage = new JSONObject();

        JSONObject jsonCatalogues = new JSONObject();
        for (Map.Entry<DownloadBean, NetCDFDownloadOutput> downloadFileEntry : downloadOutputMap.entrySet()) {
            DownloadBean downloadBean = downloadFileEntry.getKey();
            NetCDFDownloadOutput downloadOutput = downloadFileEntry.getValue();

            int nbDownloadedFiles = downloadOutput.getSuccess().size();
            int nbWarningMsgs = downloadOutput.getWarnings().size();
            int nbErrorMsgs = downloadOutput.getErrors().size();

            LOGGER.info(String.format("Download definition: %s", downloadBean.getId()));
            LOGGER.info("Downloaded files:");
            for (NetCDFMetadataBean fileMetadata : downloadOutput.getSuccess()) {
                LOGGER.info(String.format("- %s", fileMetadata.getFileURI()));
            }

            if (nbWarningMsgs > 0) {
                LOGGER.info("Warnings:");
                for (String warningMsg : downloadOutput.getWarnings()) {
                    LOGGER.info(String.format("- %s", warningMsg));
                }
            }

            if (nbErrorMsgs > 0) {
                LOGGER.info("Errors:");
                for (String errorMsg : downloadOutput.getErrors()) {
                    LOGGER.info(String.format("- %s", errorMsg));
                }
            }

            jsonCatalogues.put(
                downloadBean.getId(),
                new JSONObject()
                    .put("downloadedFiles", nbDownloadedFiles)
                    .put("warningMessages", nbWarningMsgs)
                    .put("errorMessages", nbErrorMsgs)
            );
        }
        jsonMessage.put("downloadDefinitions", jsonCatalogues);

        LOGGER.debug(String.format("Sending final SNS message: %s", jsonMessage.toString(4)));
        return this.sendNotification(jsonMessage.toString(), topicArn);
    }

    /**
     * Used to send a JSON SNS after every NetCDF file download.
     * @param downloadBean
     * @param downloadOutput
     * @return The SNS message ID
     * @throws IOException
     */
    public String sendDownloadNotification(DownloadBean downloadBean, NetCDFDownloadOutput downloadOutput) throws IOException {
        if (downloadOutput == null || downloadOutput.isEmpty()) {
            return null;
        }

        String topicArn = System.getenv(DOWNLOAD_COMPLETE_TOPIC_ARN_KEY);

        int nbDownloadedFiles = downloadOutput.getSuccess().size();
        int nbWarningMsgs = downloadOutput.getWarnings().size();
        int nbErrorMsgs = downloadOutput.getErrors().size();

        LOGGER.info(String.format("Download definition: %s", downloadBean.getId()));
        LOGGER.info("Downloaded files:");
        for (NetCDFMetadataBean fileMetadata : downloadOutput.getSuccess()) {
            LOGGER.info(String.format("- %s", fileMetadata.getFileURI()));
        }

        if (nbWarningMsgs > 0) {
            LOGGER.info("Warnings:");
            for (String warningMsg : downloadOutput.getWarnings()) {
                LOGGER.info(String.format("- %s", warningMsg));
            }
        }

        if (nbErrorMsgs > 0) {
            LOGGER.info("Errors:");
            for (String errorMsg : downloadOutput.getErrors()) {
                LOGGER.info(String.format("- %s", errorMsg));
            }
        }

        JSONObject jsonMessage = new JSONObject()
            .put("downloadDefinitionId", downloadBean.getId())
            .put("downloadedFiles", nbDownloadedFiles)
            .put("warningMessages", nbWarningMsgs)
            .put("errorMessages", nbErrorMsgs);

        LOGGER.debug(String.format("Sending SNS message: %s", jsonMessage.toString(4)));
        return this.sendNotification(jsonMessage.toString(), topicArn);
    }


    /**
     * Send an AWS notification
     * @param message The message to send in the SNS.
     * @param topicArn The SNS topic ARN.
     * @return The SNS message ID
     * @throws IOException
     */
    private String sendNotification(String message, String topicArn) throws IOException {
        // The message is a stringify JSON object that needs to be parsed by the listeners
        PublishRequest publishRequest = new PublishRequest(topicArn, message);

        PublishResult publishResult = this.snsClient.publish(publishRequest);

        String messageId = publishResult.getMessageId();
        if (messageId == null || messageId.isEmpty()) {
            throw new IOException(String.format("Could not send the notification for topic \"%s\".%nMessage:%n%s",
                    topicArn, message));
        }

        return messageId;
    }
}
