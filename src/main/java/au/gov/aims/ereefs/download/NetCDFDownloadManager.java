/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.aws.s3.FileWrapper;
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.manager.UploadManager;
import au.gov.aims.ereefs.Utils;
import au.gov.aims.ereefs.ZipUtils;
import au.gov.aims.ereefs.bean.AbstractBean;
import au.gov.aims.ereefs.bean.NetCDFUtils;
import au.gov.aims.ereefs.bean.download.CatalogueUrlBean;
import au.gov.aims.ereefs.bean.download.DownloadBean;
import au.gov.aims.ereefs.bean.download.OutputBean;
import au.gov.aims.ereefs.bean.metadata.netcdf.NetCDFMetadataBean;
import au.gov.aims.ereefs.database.CacheStrategy;
import au.gov.aims.ereefs.database.DatabaseClient;
import au.gov.aims.ereefs.database.manager.DownloadManager;
import au.gov.aims.ereefs.database.manager.MetadataManager;
import au.gov.aims.ereefs.helper.DownloadHelper;
import au.gov.aims.ereefs.helper.MetadataHelper;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.internal.Constants;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import thredds.client.catalog.Access;
import thredds.client.catalog.Catalog;
import thredds.client.catalog.Dataset;
import thredds.client.catalog.ServiceType;
import thredds.client.catalog.builder.CatalogBuilder;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.units.DateType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 1. Load / parse the Thredds XML catalogue,
 * 2. Compare the list of files in the XML with the list of files already downloaded
 *     (by looking at JSON metadata documents in the DB),
 * 3. Download the NetCDF files and create the JSON metadata document for each downloaded file.
 */
public class NetCDFDownloadManager {
    private static final Logger LOGGER = Logger.getLogger(NetCDFDownloadManager.class);
    private static final String APP_NAME = "downloadManager";
    private static final long MAX_DOWNLOAD_FILE_SIZE = 100L * 1024 * 1024 * 1024; // 100 GB, in Bytes

    // Control the "wait and retry" delay between retries.
    // Attempt | Wait
    //  1      |    0 seconds
    //  2      |   10 seconds
    //  3      |   20 seconds
    //  4      |   40 seconds
    //  5      |   80 seconds (~1 minute)
    //  6      |  160 seconds (~2.5 minutes)
    //  7      |  320 seconds (~5 minutes)
    //  8      |  640 seconds (~10 minutes)
    //  9      | 1280 seconds (~21 minutes)
    // 10      | 2540 seconds (~42 minutes)
    // 11      | 5120 seconds (~85 minutes)
    private static final int DOWNLOAD_RETRY_INITIAL_WAIT = 10; // in seconds
    private static final int MAX_DOWNLOAD_RETRY = 8;

    // Number of file to download in each Download definition.
    //     Use negative number to download everything.
    private static final int DEFAULT_DOWNLOAD_LIMIT = -1;

    private static final int HTTP_CLIENT_TIMEOUT = 5 * 60 * 1000; // 5 minutes, in milliseconds

    private DownloadBean downloadBean;
    private List<CatalogueEntry> catalogues;

    // http://dapds00.nci.org.au/thredds/catalog/fx3/$model_name/catalog.xml
    // http://dapds00.nci.org.au/thredds/catalog/fx3/gbr4_v2/catalog.xml

    // http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0

    public NetCDFDownloadManager(DownloadBean downloadBean) {
        this.downloadBean = downloadBean;
    }

    /**
     * Run the download manager.
     *
     * <p>List of supported arguments: See parameter <em>args</em> bellow.</p>
     *
     * <p>NOTE: Environment variables can be used</p>
     *
     * <p>List of supported environment variables:</p>
     * <ul>
     *     <li><em>DRYRUN</em>: Dryrun mode - list all files that would be downloaded without downloading them.
     *         "true" to run in "dryrun" mode,
     *         "false" to actually download the files.
     *         anything else: Show a warning message and run in "dryrun", to be safe.
     *         Default: false.</li>
     *     <li><em>LIMIT</em>: Integer.
     *         Positive integer to only download X file per download definition,
     *         0 to download nothing,
     *         Negative integer to download everything.
     *         Default: -1.</li>
     *     <li><em>DOWNLOADDEFINITIONID</em>: String.
     *         The download definition to download. Can be a disabled download definition.
     *         Default: All enabled download definitions.</li>
     * </ul>
     *
     * @param args parameters
     *     Use parameter "dryrun" to list NetCDF files that would be downloaded, without downloading anything.
     *         NOTE: This is not a key-value pair parameter. Dryrun mode is activated when the parameter is present.
     *         Example: "java -jar ereefs-download-manager-jar-with-dependencies.jar dryrun".
     *     Use parameter "limit" to limit the amount of files downloaded.
     *         Positive integer to only download X file per download definition,
     *         0 to download nothing,
     *         Negative integer to download everything.
     *         Default: -1.
     *         Example: "java -jar ereefs-download-manager-jar-with-dependencies.jar limit 2"
     *     Use parameter "downloaddefinitionid" to specify the download definition to download.
     *         Doesn't work with disabled download definitions.
     *         Default: All enabled download definitions.
     *     Use parameter "files" to specify a comma separated list of file to be downloaded.
     *         Can only be used with parameter "downloaddefinitionid".
     */
    public static void main(String ... args) throws Exception {
        boolean dryRun = false;
        int limit = DEFAULT_DOWNLOAD_LIMIT;

        // Used when manually downloading files for a specific download definition
        String downloadDefinitionId = null;

        // Used to manually specify files to be downloaded
        String filesStr = null;
        List<String> files = null;

        // Set dryRun and limit using environment variables
        String envDryRunStr = System.getenv("DRYRUN");
        if (envDryRunStr != null && !envDryRunStr.isEmpty()) {
            if (envDryRunStr.equalsIgnoreCase("false")) {
                dryRun = false;
            } else if (envDryRunStr.equalsIgnoreCase("true")) {
                dryRun = true;
            } else {
                // If dryRun is false, it will run for real. Otherwise, it will do a dryRun
                LOGGER.warn("Invalid DRYRUN environment variable. Expected TRUE or FALSE. Found: " + envDryRunStr);

                dryRun = true;
                LOGGER.warn("Dryrun was set to true (to be safe)");
            }
        }
        String envLimitStr = System.getenv("LIMIT");
        if (envLimitStr != null && !envLimitStr.isEmpty()) {
            try {
                limit = Integer.parseInt(envLimitStr);
            } catch (Exception ex) {
                LOGGER.warn("Invalid LIMIT environment variable. Expected integer. Found: " + envLimitStr, ex);
            }
        }
        downloadDefinitionId = System.getenv("DOWNLOADDEFINITIONID");

        filesStr = System.getenv("FILES");

        // Set dryRun and limit using arguments
        if (args != null && args.length > 0) {
            for (int i=0; i<args.length; i++) {
                String arg = args[i];
                if ("dryrun".equalsIgnoreCase(arg)) {
                    dryRun = true;
                }
                if ("limit".equalsIgnoreCase(arg)) {
                    if (++i < args.length) {
                        String limitStr = args[i];
                        try {
                            limit = Integer.parseInt(limitStr);
                        } catch (Exception ex) {
                            LOGGER.warn("Invalid limit parameter. Expected integer. Found: " + envLimitStr, ex);
                        }
                    }
                }
                if ("downloaddefinitionid".equalsIgnoreCase(arg)) {
                    if (++i < args.length) {
                        downloadDefinitionId = args[i];
                    }
                }
                if ("files".equalsIgnoreCase(arg)) {
                    if (++i < args.length) {
                        filesStr = args[i];
                    }
                }
            }
        }

        if (downloadDefinitionId != null) {
            downloadDefinitionId = downloadDefinitionId.trim();
            if (downloadDefinitionId.isEmpty() || downloadDefinitionId.equalsIgnoreCase("null")) {
                downloadDefinitionId = null;
            }
        }

        if (filesStr != null && !filesStr.isEmpty()) {
            files = new ArrayList<String>();
            for (String fileStr : filesStr.split(",")) {
                String cleanFileStr = fileStr.trim();
                if (!cleanFileStr.isEmpty()) {
                    files.add(cleanFileStr);
                }
            }
        }

        DatabaseClient dbClient = new DatabaseClient(APP_NAME);

        DownloadHelper downloadHelper = new DownloadHelper(dbClient, CacheStrategy.DISK);

        LOGGER.info("DownloadManager task summary:");
        if (downloadDefinitionId != null) {
            LOGGER.info(String.format("- downloadDefinitionId: %s", downloadDefinitionId));
        }
        if (files != null && !files.isEmpty()) {
            LOGGER.info(String.format("- files: %s", StringUtils.join(files, ", ")));
        }
        LOGGER.info(String.format("- limit: %d", limit));
        LOGGER.info(String.format("- dryRun: %s", dryRun));

        Iterable<DownloadBean> threddsCatalogueBeans = null;
        if (downloadDefinitionId != null) {
            DownloadManager downloadManager = new DownloadManager(dbClient, CacheStrategy.DISK);
            JSONObject jsonDownloadDefinition = downloadManager.select(downloadDefinitionId);

            if (jsonDownloadDefinition != null) {
                // Add files, if any are specified
                if (files != null && !files.isEmpty()) {
                    jsonDownloadDefinition.put("files", files);
                }

                // Create an iterator capable of returning a single DownloadBean
                threddsCatalogueBeans = new Iterable<DownloadBean>() {
                    public Iterator<DownloadBean> iterator() {
                        return new Iterator<DownloadBean>() {
                            private boolean visited = false;

                            public boolean hasNext() {
                                return !this.visited;
                            }

                            public DownloadBean next() {
                                this.visited = true;
                                return new DownloadBean(jsonDownloadDefinition);
                            }
                        };
                    }
                };
            }
        } else {
            try {
                threddsCatalogueBeans = downloadHelper.getEnabledDownloads();
            } catch (Exception ex) {
                throw new Exception(String.format("Exception occurred while loading the list of catalogues from the database: %s",
                        Utils.getExceptionMessage(ex)), ex);
            }
        }

        if (threddsCatalogueBeans == null) {
            LOGGER.warn("There is no active download definition. The database returned null.");
        } else {
            Map<DownloadBean, NetCDFDownloadOutput> downloadOutputMap = new HashMap<DownloadBean, NetCDFDownloadOutput>();

            // For each enabled DownloadDefinition found in the Database DOWNLOAD table:
            try (NotificationManager notificationManager = new NotificationManager()) {
                int catalogueCount = 0;
                for (DownloadBean downloadBean : threddsCatalogueBeans) {
                    NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

                    // Download the NetCDF file,
                    //     extract its metadata,
                    //     save it to the database,
                    //     upload the file to S3 and
                    //     delete downloaded files
                    NetCDFDownloadOutput downloadedOutput =
                            downloadManager.download(dbClient, notificationManager, dryRun, limit);
                    if (downloadedOutput != null && !downloadedOutput.isEmpty()) {
                        downloadOutputMap.put(downloadManager.downloadBean, downloadedOutput);
                    }

                    catalogueCount++;
                }

                if (catalogueCount <= 0) {
                    LOGGER.warn("There is no active download definition");
                }

                if (!downloadOutputMap.isEmpty()) {
                    if (notificationManager != null) {
                        // Send Notification
                        try {
                            notificationManager.sendFinalDownloadNotification(downloadOutputMap);
                        } catch(Throwable ex) {
                            LOGGER.error("Error occurred while sending the final download notification about all downloaded files.", ex);
                        }
                    }
                }
            }
        }
    }

    /**
     * Download missing or outdated files.
     *
     * 1. Download the Thredds XML catalogue
     * 2. Compare the lastModified date of all the dataset (NetCDF file entry) from the catalogue
     *     with the lastDownloaded date in the database
     * 3. For each dataset, if the dataset is not in the database OR the version in the database is outdated:
     *     3.1 Download the NetCDF file to disk with extension ".downloading"
     *     3.2 Validate the file and generate the NetCDF file metadata object
     *         3.3.1 If the file is corrupted (the validation failed or the metadata generation failed),
     *               create a "file is corrupted" metadata and delete the downloaded file.
     *         3.3.2 If the file is not corrupted, remove the file extension ".downloading"
     *     3.3 Upload the file to S3 (if not corrupted)
     *     3.4 Save the metadata in the database.
     * NOTE: The metadata is saved in the database at the end, so it only occurs when everything worked as expected
     *     (no exceptions thrown).
     */
    public NetCDFDownloadOutput download(DatabaseClient dbClient, NotificationManager notificationManager, boolean dryRun) throws Exception {
        return this.download(dbClient, notificationManager, dryRun, -1);
    }

    /**
     * See {@link #download(DatabaseClient, NotificationManager, boolean)}
     *
     * @param dbClient
     * @param notificationManager
     * @param dryRun
     * @param limit Positive integer to only download X file per download definition. 0 to download nothing. Negative integer to download everything.
     * @return NetCDFDownloadOutput to describe which files was downloaded and which one failed.
     */
    public NetCDFDownloadOutput download(
            DatabaseClient dbClient,
            NotificationManager notificationManager,
            boolean dryRun, int limit) throws Exception {

        if (this.downloadBean == null) {
            LOGGER.warn("Can not download the datasets, the catalogue is null.");
            return null;
        }
        if (!this.downloadBean.isEnabled()) {
            LOGGER.warn(String.format("Can not download the datasets from the catalogue %s, the catalogue is disabled.",
                    this.downloadBean.getId()));
            return null;
        }

        OutputBean catalogueOutputBean = this.downloadBean.getOutput();
        if (catalogueOutputBean == null) {
            LOGGER.warn(String.format("Can not download the datasets from the catalogue %s, the catalogue has no output defined.",
                    this.downloadBean.getId()));
            return null;
        }

        // Get old NetCDF metadata
        MetadataManager metadataManager = new MetadataManager(dbClient, CacheStrategy.DISK);
        MetadataHelper metadataHelper = new MetadataHelper(dbClient, CacheStrategy.DISK);

        // Get new NetCDF dataset (described in the catalog.xml)
        Map<String, DatasetEntry> newDatasetEntryMap = null;
        try {
            newDatasetEntryMap = this.getDatasets();
        } catch(OutOfMemoryError outOfMemory) {
            LOGGER.error("Out Of Memory while loading the list of dataset from catalog.xml", outOfMemory);
            throw outOfMemory;
        } catch(Throwable ex) {
            LOGGER.error(String.format("Can not extract files from catalogue for download definition ID: %s",
                    this.downloadBean.getId()), ex);
            return null;
        }

        if (newDatasetEntryMap == null || newDatasetEntryMap.isEmpty()) {
            LOGGER.error(String.format("No suitable catalogue URL found for download definition ID: %s",
                    this.downloadBean.getId()));
            return null;
        }

        OutputBean.Type catalogueOutputType = catalogueOutputBean.getType();

        // Get the list of dataset ID and order them alphabetically
        // to download them in chronological order.
        // This makes logs easier to read.
        List<String> sortedDatasetIds = new ArrayList<String>(newDatasetEntryMap.keySet());
        Collections.sort(sortedDatasetIds);

        NetCDFDownloadOutput downloadOutput = new NetCDFDownloadOutput();
        if (limit != 0) {
            String catalogueId = this.downloadBean.getId();

            // Create a map of metadata keyed with dataset ID, to avoid requesting metadata for every dataset.
            Map<String, NetCDFMetadataBean> oldMetadataMap = new HashMap<String, NetCDFMetadataBean>();
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(catalogueId)) {
                oldMetadataMap.put(metadataBean.getDatasetId(), metadataBean);
            }

            int counter = limit;
            try {
                for (String datasetId : sortedDatasetIds) {
                    DatasetEntry newDatasetEntry = newDatasetEntryMap.get(datasetId);
                    Dataset newDataset = newDatasetEntry.getDataset();

                    String uniqueDatasetId = AbstractBean.safeIdValue(NetCDFMetadataBean.getUniqueDatasetId(catalogueId, datasetId));
                    long newLastModified = NetCDFDownloadManager.convertDateTypeToTimestamp(newDataset.getLastModifiedDate());

                    NetCDFMetadataBean oldMetadata = oldMetadataMap.get(datasetId);

                    URI fileUri = NetCDFDownloadManager.getDatasetFileURI(newDataset);

                    // Find where the file will be uploaded
                    // Examples:
                    //     s3://bucket/file.nc
                    //     file://directory/file.nc
                    URI destinationURI = this.getDestinationURI(newDatasetEntry);
                    if (destinationURI == null) {
                        LOGGER.error("Destination URI is null");
                        downloadOutput.addError(String.format("Destination URI is null for file URI: %s", fileUri));
                        return downloadOutput;
                    }

                    boolean outdated = true;
                    if (oldMetadata != null) {
                        long oldMetadataLastModified = oldMetadata.getLastModified();

                        // Compare lastModified dates
                        if (newLastModified <= oldMetadataLastModified) {
                            outdated = false;
                            LOGGER.debug(String.format("The NetCDF file ID %s is up to date.", uniqueDatasetId));
                        } else {
                            LOGGER.info(String.format("The NetCDF file ID %s is outdated. Last modified found in metadata: %s, last modified found in the XML catalogue: %s",
                                    uniqueDatasetId,
                                    oldMetadataLastModified,
                                    newLastModified));
                        }
                    } else {
                        LOGGER.info(String.format("The NetCDF file ID %s has no metadata in the Metadata table", uniqueDatasetId));
                    }

                    if (!outdated) {
                        // The latest downloaded file we have is not outdated
                        // Check if it has been deleted from S3
                        this.verifyDataset(metadataManager, oldMetadata, destinationURI);

                    } else {
                        // The file we have is outdated
                        // Download the NetCDF file from the Thredds server
                        Boolean downloadSuccess = this.downloadDataset(
                                metadataManager,
                                notificationManager,
                                oldMetadata,
                                catalogueId, datasetId,
                                catalogueOutputType,
                                newDataset,
                                newLastModified,
                                fileUri,
                                destinationURI,
                                downloadOutput,
                                dryRun);

                        // If the downloadDataset method returned null, something went very wrong.
                        // Stop execution.
                        if (downloadSuccess == null) {
                            return downloadOutput;
                        }

                        // Limit the number of files to attempt to download.
                        // This is used for testing.
                        // NOTE: If limit is negative, it will not stop. It will download all available files.
                        if (limit > 0 && downloadSuccess) {
                            counter--;
                            if (counter <= 0) {
                                break;
                            }
                        }
                    }
                } // for loop
            } finally {
                if (notificationManager != null && !downloadOutput.isEmpty()) {
                    // Send Notification
                    try {
                        notificationManager.sendDownloadNotification(this.downloadBean, downloadOutput);
                    } catch(Throwable ex) {
                        LOGGER.error("Error occurred while sending single downloaded file notification.", ex);
                    }
                }
            }
        }

        return downloadOutput;
    }

    private void verifyDataset(
            MetadataManager metadataManager,
            NetCDFMetadataBean oldMetadata,
            URI destinationURI) throws Exception {

        // The latest downloaded file we have is not outdated
        // Check if it has been deleted from S3
        NetCDFMetadataBean.Status oldMetadataStatus = oldMetadata.getStatus();
        if (!NetCDFMetadataBean.Status.DELETED.equals(oldMetadataStatus) &&
                !NetCDFMetadataBean.Status.CORRUPTED.equals(oldMetadataStatus)) {

            FileWrapper destinationFileWrapper = new FileWrapper(destinationURI, null);
            boolean fileExists = false;

            String scheme = destinationURI.getScheme();
            if ("file".equalsIgnoreCase(scheme)) {
                // NOTE: This is necessary for unit tests...
                fileExists = destinationFileWrapper.exists(null);
            } else {
                try (S3Client s3Client = new S3Client()) {
                    fileExists = destinationFileWrapper.exists(s3Client);
                }
            }

            if (!fileExists) {
                // It has been deleted from S3 and it's not flagged as Deleted.
                // It's time to fix this...
                oldMetadata.setStatus(NetCDFMetadataBean.Status.DELETED);
                metadataManager.save(oldMetadata.toJSON(), false);
            }
        }
    }

    // Return value:
    //   false = Nothing was downloaded.
    //   true = The dataset was successfully downloaded.
    //   null = A critical error was logged in the downloadOutput and the execution needs to be halted.
    // NOTE: Errors are logged in downloadOutput.
    private Boolean downloadDataset(
            MetadataManager metadataManager,
            NotificationManager notificationManager,
            NetCDFMetadataBean oldMetadata,
            String catalogueId, String datasetId,
            OutputBean.Type catalogueOutputType,
            Dataset newDataset,
            long newLastModified,
            URI fileUri,
            URI destinationURI,
            NetCDFDownloadOutput downloadOutput,
            boolean dryRun) throws Exception {

        // The file we have is outdated
        // Download the NetCDF file from the Thredds server
        File downloadFile = this.getDownloadFile(newDataset);

        // Check if there is enough space left of the device to download the file
        long fileSize = newDataset.getDataSize();
        double fileSizeMB = (double)fileSize / Constants.MB;

        File downloadDir = downloadFile.getParentFile();
        if (!downloadDir.exists()) {
            if (!downloadDir.mkdirs()) {
                LOGGER.error(String.format("Can not create the directory for the temporary file %s", downloadDir));
                downloadOutput.addError(String.format("Can not create the directory for the temporary file %s, file URI: %s", downloadDir, fileUri));
                return null;
            }
        }
        long freeSpace = downloadDir.getUsableSpace();
        double freeSpaceMB = (double)freeSpace / Constants.MB;

        LOGGER.info(String.format("Space left on %s (%.1f MB available) before downloading %s (%.1f MB)",
                downloadDir, freeSpaceMB, fileUri, fileSizeMB));

        // Can't download this file, not enough available disk space.
        if (fileSize > freeSpace) {
            LOGGER.error(String.format("There is not enough space left on %s (%.1f MB available) to download %s (%.1f MB)",
                    downloadDir, freeSpaceMB, fileUri, fileSizeMB));

            if (notificationManager != null) {
                // Send Notification
                try {
                    notificationManager.sendDiskFullNotification(fileUri, fileSizeMB, freeSpaceMB);
                } catch(Throwable ex) {
                    LOGGER.error("Error occurred while sending disk full notification.", ex);
                }
            }

            // Go to the next file. Hopefully it's smaller and can be downloaded
            downloadOutput.addWarning(String.format("Not enough disk space to download the file URI: %s. File size: %f, free space: %f", fileUri, fileSizeMB, freeSpaceMB));
            return false;
        }

        Boolean downloadSuccess = null;
        if (dryRun) {
            System.out.println(String.format("DRY RUN: URL \"%s\" will be download to \"%s\" (%.1f MB)",
                    fileUri, destinationURI, fileSizeMB));

            downloadSuccess = true;

        } else {
            try {
                downloadSuccess = this.downloadDatasetFile(downloadFile,
                        metadataManager,
                        notificationManager,
                        oldMetadata,
                        catalogueId, datasetId,
                        catalogueOutputType,
                        newLastModified,
                        fileUri,
                        destinationURI,
                        downloadOutput);

            } finally {
                if (downloadFile.exists()) {
                    LOGGER.debug("Deleting temporary file: " + downloadFile);
                    if (!downloadFile.delete()) {
                        LOGGER.error(String.format("Can not delete the temporary file %s", downloadFile.getAbsolutePath()));
                    }
                }
            }
        }

        // If success is null, something went really wrong.
        // Stop execution.

        // If the download failed, return false.
        // Let it try with the next one.

        return downloadSuccess;
    }

    // Return value:
    //   false = Nothing was downloaded.
    //   true = The dataset file was successfully downloaded.
    //   null = A critical error was logged in the downloadOutput and the execution needs to be halted.
    // NOTE: Errors are logged in downloadOutput.
    private Boolean downloadDatasetFile(
            File downloadFile,
            MetadataManager metadataManager,
            NotificationManager notificationManager,
            NetCDFMetadataBean oldMetadata,
            String catalogueId, String datasetId,
            OutputBean.Type catalogueOutputType,
            long newLastModified,
            URI fileUri,
            URI destinationURI,
            NetCDFDownloadOutput downloadOutput) throws Exception {

        File downloadDir = downloadFile.getParentFile();

        LOGGER.debug("Before downloading: " + downloadFile);
        LOGGER.debug(String.format("    Total space: %d MB", downloadDir.getTotalSpace() / Constants.MB));
        LOGGER.debug(String.format("    Free space: %d MB", downloadDir.getFreeSpace() / Constants.MB));
        LOGGER.debug(String.format("    Usable space: %d MB", downloadDir.getUsableSpace() / Constants.MB));

        try {
            NetCDFDownloadManager.downloadURIToFile(fileUri, downloadFile);
        } catch(OutOfMemoryError outOfMemory) {
            LOGGER.error("Out Of Memory while downloading the NetCDF file", outOfMemory);
            throw outOfMemory;
        } catch(Throwable ex) {
            LOGGER.error(String.format("Error occurred while download the file URI %s to disk %s", fileUri, downloadFile), ex);
            downloadOutput.addError(String.format("Error occurred while download the file URI %s to disk %s", fileUri, downloadFile));
            return false;
        }

        // Unzip the file, if needed
        if (downloadFile.exists() && ZipUtils.isZipped(downloadFile.getName())) {
            File unzippedDownloadFile = ZipUtils.unzipFile(downloadFile);
            downloadFile.delete();
            downloadFile = unzippedDownloadFile;
        }

        // If the NetCDF file was downloaded
        Boolean fileIsValid = null;
        if (downloadFile.exists()) {

            fileIsValid = this.verifyDownloadedDatasetFile(
                        downloadFile,
                        metadataManager,
                        notificationManager,
                        oldMetadata,
                        catalogueId, datasetId,
                        catalogueOutputType,
                        newLastModified,
                        fileUri,
                        destinationURI,
                        downloadOutput);

        } else {
            // The downloaded file doesn't exist
            // Should not happen
            LOGGER.error(String.format("The NetCDF file %s found at URI %s was not downloaded for unknown reason.",
                    downloadFile.getName(), fileUri));
            downloadOutput.addWarning(String.format("Could not download file URI: %s", fileUri));
            fileIsValid = null;
        }

        return fileIsValid;
    }

    // Verify downloaded file and save file metadata in the database.
    // Return value:
    //   false = The downloaded file is invalid.
    //   true = The downloaded file is valid.
    //   null = A critical error was logged in the downloadOutput and the execution needs to be halted.
    // NOTE: Errors are logged in downloadOutput.
    private Boolean verifyDownloadedDatasetFile(
            File downloadFile,
            MetadataManager metadataManager,
            NotificationManager notificationManager,
            NetCDFMetadataBean oldMetadata,
            String catalogueId, String datasetId,
            OutputBean.Type catalogueOutputType,
            long newLastModified,
            URI fileUri,
            URI destinationURI,
            NetCDFDownloadOutput downloadOutput) throws Exception {

        File downloadDir = downloadFile.getParentFile();

        // Create the metadata for the downloaded NetCDF file
        NetCDFMetadataBean newMetadata = NetCDFMetadataBean.create(catalogueId, datasetId, destinationURI, downloadFile, newLastModified);
        if (newMetadata == null) {
            // Should not happen
            LOGGER.error(String.format("Can not generate metadata for file URI: %s, download file: %s", fileUri, downloadFile));
            downloadOutput.addWarning(String.format("Can not generate metadata for file URI: %s", fileUri));
            return false;
        } else {
            newMetadata.setLastDownloaded(System.currentTimeMillis());
            if (NetCDFMetadataBean.Status.VALID.equals(newMetadata.getStatus())) {
                // Compare file MD5
                String oldChecksum = oldMetadata == null ? null : oldMetadata.getChecksum();
                String newChecksum = newMetadata.getChecksum();
                if (newChecksum != null && newChecksum.equals(oldChecksum)) {
                    oldMetadata.setLastDownloaded(System.currentTimeMillis());
                    oldMetadata.setLastModified(newLastModified);
                    metadataManager.save(oldMetadata.toJSON(), false);
                } else {
                    String errorMessage = null;
                    try {
                        errorMessage = NetCDFUtils.scanWithErrorMessage(downloadFile);
                    } catch(OutOfMemoryError outOfMemory) {
                        LOGGER.error(String.format("Out Of Memory while scanning the NetCDF file %s found at URL %s.",
                                downloadFile.getName(), fileUri), outOfMemory);
                        throw outOfMemory;
                    } catch (Throwable ex) {
                        LOGGER.error(String.format("Error occurred while scanning the NetCDF file %s found at URL %s",
                                downloadFile.getName(), fileUri), ex);

                        if (notificationManager != null) {
                            // Send Notification
                            try {
                                notificationManager.sendCorruptedFileNotification(fileUri, Utils.getExceptionMessage(ex));
                            } catch(Throwable ex2) {
                                LOGGER.error("Error occurred while sending corrupted file notification.", ex2);
                            }
                        }

                        newMetadata.setStatus(NetCDFMetadataBean.Status.CORRUPTED);
                        newMetadata.setErrorMessage("Error occurred during data scan");
                        newMetadata.setStacktrace(ex);
                        metadataManager.save(newMetadata.toJSON());

                        downloadOutput.addWarning(String.format("Error occurred during data scan for file URI: %s", fileUri));
                        return false;
                    }

                    if (errorMessage != null) {
                        String detailedErrorMessage = String.format("The NetCDF file %s found at URL %s contains invalid / corrupted data: %s",
                                downloadFile.getName(), fileUri, errorMessage);
                        LOGGER.error(detailedErrorMessage);

                        if (notificationManager != null) {
                            // Send Notification
                            try {
                                notificationManager.sendCorruptedFileNotification(fileUri, detailedErrorMessage);
                            } catch(Throwable ex2) {
                                LOGGER.error("Error occurred while sending corrupted file notification.", ex2);
                            }
                        }

                        newMetadata.setStatus(NetCDFMetadataBean.Status.CORRUPTED);
                        newMetadata.setErrorMessage(detailedErrorMessage);
                        metadataManager.save(newMetadata.toJSON());
                        downloadOutput.addWarning(detailedErrorMessage);
                    } else {
                        LOGGER.info(String.format("Uploading %s to %s",
                                downloadFile, destinationURI));

                        switch (catalogueOutputType) {
                            case S3:
                                // Upload the NetCDF file to S3, if needed
                                try {
                                    AmazonS3URI s3URI = new AmazonS3URI(destinationURI);
                                    this.uploadToS3(downloadFile, s3URI);
                                } catch(OutOfMemoryError outOfMemory) {
                                    LOGGER.error("Out Of Memory while uploading the NetCDF file to S3", outOfMemory);
                                    throw outOfMemory;
                                } catch(Throwable ex) {
                                    LOGGER.error(String.format("Error occurred while uploading the file %s from URI %s to S3 %s", downloadFile, fileUri, destinationURI), ex);
                                    downloadOutput.addError(String.format("Error occurred while uploading the file URI %s to S3 %s", fileUri, destinationURI));
                                    return false;
                                } finally {

                                    LOGGER.debug("Before deleting: " + downloadFile);
                                    LOGGER.debug(String.format("    Total space: %d MB", downloadDir.getTotalSpace() / Constants.MB));
                                    LOGGER.debug(String.format("    Free space: %d MB", downloadDir.getFreeSpace() / Constants.MB));
                                    LOGGER.debug(String.format("    Usable space: %d MB", downloadDir.getUsableSpace() / Constants.MB));

                                    if (!downloadFile.delete()) {
                                        LOGGER.error(String.format("Can not delete the downloaded file %s", downloadFile.getAbsolutePath()));
                                    }

                                    LOGGER.debug("After deleting: " + downloadFile);
                                    LOGGER.debug(String.format("    Total space: %d MB", downloadDir.getTotalSpace() / Constants.MB));
                                    LOGGER.debug(String.format("    Free space: %d MB", downloadDir.getFreeSpace() / Constants.MB));
                                    LOGGER.debug(String.format("    Usable space: %d MB", downloadDir.getUsableSpace() / Constants.MB));
                                }
                                break;

                            case FILE:
                                // Move the downloaded file to its final destination
                                File destinationFile = new File(destinationURI);
                                destinationFile.getParentFile().mkdirs();
                                if (!downloadFile.renameTo(destinationFile)) {
                                    LOGGER.error(String.format("Can not rename the downloaded file %s to %s.", downloadFile, destinationFile));
                                    downloadOutput.addError(String.format("Can not rename the downloaded file %s to %s.", downloadFile, destinationFile));
                                    return false;
                                }
                                break;

                            default:
                                LOGGER.error(String.format("Invalid destination URI: %s for file URI: %s", destinationURI, fileUri));
                                downloadOutput.addError(String.format("Invalid destination URI: %s for file URI: %s", destinationURI, fileUri));
                                return false;
                        }

                        // Save the NetCDF file metadata into the database
                        metadataManager.save(newMetadata.toJSON());
                        downloadOutput.addSuccess(newMetadata);
                    }
                }
            } else {
                metadataManager.save(newMetadata.toJSON());
                downloadOutput.addWarning(String.format("Metadata is invalid for file URI: %s", fileUri));

                // The downloaded file is NOT valid. Delete it
                LOGGER.warn(String.format("The NetCDF file %s found at URL %s is corrupted.",
                        downloadFile.getName(), fileUri));

                String errorMessage = newMetadata.getErrorMessage();
                if (errorMessage != null) {
                    LOGGER.error(String.format("Error: %s", errorMessage));
                }
                List<String> stacktrace = newMetadata.getStacktrace();
                if (stacktrace != null) {
                    for (String stacktraceLine : stacktrace) {
                        LOGGER.error(String.format("    %s", stacktraceLine));
                    }
                }

                if (notificationManager != null) {
                    // Send Notification
                    try {
                        notificationManager.sendCorruptedFileNotification(fileUri, errorMessage, newMetadata.getException());
                    } catch(Throwable ex) {
                        LOGGER.error("Error occurred while sending corrupted file notification.", ex);
                    }
                }

                if (!downloadFile.delete()) {
                    LOGGER.error(String.format("Can not delete the downloaded file %s.", downloadFile));
                    return null;
                }
            }
        }

        return true;
    }

    public static long convertDateTypeToTimestamp(DateType dateType) {
        if (dateType == null) {
            return 0;
        }

        CalendarDate calDate = dateType.getCalendarDate();
        if (calDate == null) {
            return 0;
        }

        return calDate.getMillis();
    }

    /**
     * Parse the THREDDS Catalogue URLs.
     *
     * NOTE: Each download manager definition may define multiple catalogue URLs.
     *     They usually define a single URL. This object only represent a single
     *     download manager definition.
     * @return
     * @throws Exception
     */
    public List<CatalogueEntry> getCatalogues() throws Exception {
        if (this.catalogues == null) {
            this.catalogues = new ArrayList<CatalogueEntry>();

            List<CatalogueUrlBean> catalogueUrlBeans = this.downloadBean.getCatalogueUrls();
            if (catalogueUrlBeans != null && !catalogueUrlBeans.isEmpty()) {
                for (CatalogueUrlBean catalogueUrlBean : catalogueUrlBeans) {
                    if (catalogueUrlBean == null) {
                        return null;
                    }

                    URL catalogueUrl = catalogueUrlBean.getCatalogueUrl();
                    if (catalogueUrl == null) {
                        return null;
                    }

                    URI catalogueUri = catalogueUrl.toURI();

                    CatalogBuilder catalogBuilder = new CatalogBuilder();

                    String protocol = catalogueUri.getScheme();
                    if ("http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol)) {

                        // Download the THREDDS catalogue.
                        // NOTE: This could be achieve with the following line of code:
                        //         this.catalogue = catalogBuilder.buildFromURI(catalogueUri);
                        //     Unfortunately, the "buildFromURI" method doesn't have
                        //     configuration option for "timeout" and times out while
                        //     trying to download eReefs GBR1 v2 catalogue.

                        HttpGet httpGet = null;
                        try (CloseableHttpClient httpClient = NetCDFDownloadManager.openHttpClient()) {
                            httpGet = new HttpGet(catalogueUri);
                            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                                StatusLine httpStatus = response.getStatusLine();
                                if (httpStatus != null) {
                                    int statusCode = httpStatus.getStatusCode();
                                    if (statusCode < 200 || statusCode >= 300) {
                                        LOGGER.warn(String.format("Error occurred while downloading the THREDDS Catalogue %s: %s (%d)",
                                                catalogueUri, httpStatus.getReasonPhrase(), statusCode));
                                        continue;
                                    }
                                }

                                // The entity is streamed
                                HttpEntity entity = response.getEntity();
                                if (entity == null) {
                                    throw new IOException(String.format("Can not use the downloaded THREDDS Catalogue %s: The response entity is null",
                                            catalogueUri));
                                } else {
                                    try (InputStream in = entity.getContent()) {
                                        this.catalogues.add(
                                                new CatalogueEntry(catalogBuilder.buildFromStream(in, catalogueUri), catalogueUrlBean));
                                    }
                                }
                            }
                        } finally {
                            if (httpGet != null) {
                                // Cancel the connection, if it's still alive
                                httpGet.abort();
                                // Close connections
                                httpGet.reset();
                            }
                        }

                    } else {
                        this.catalogues.add(new CatalogueEntry(catalogBuilder.buildFromURI(catalogueUri), catalogueUrlBean));
                    }
                }
            }
        }

        return this.catalogues;
    }

    // Get Dataset from THREDDS catalogues
    public Map<String, DatasetEntry> getDatasets() throws Exception {
        List<CatalogueEntry> catalogues = this.getCatalogues();
        if (catalogues == null || catalogues.isEmpty()) {
            return null;
        }

        Pattern regex = this.downloadBean.getFilenameRegex();
        Set<String> files = this.downloadBean.getFiles();
        Map<String, DatasetEntry> filteredDatasets = new HashMap<String, DatasetEntry>();
        for (CatalogueEntry catalogueEntry : catalogues) {
            Catalog catalogue = catalogueEntry.getCatalogue();

            Iterable<Dataset> datasets = catalogue.getAllDatasets();
            for (Dataset dataset : datasets) {
                String urlPath = dataset.getUrlPath();
                if (urlPath != null && !urlPath.isEmpty()) {
                    boolean selected = false;
                    if (files != null && !files.isEmpty()) {
                        String filename = FilenameUtils.getName(dataset.getUrlPath());
                        if (files.contains(filename)) {
                            selected = true;
                        }
                    } else if (regex != null) {
                        String filename = FilenameUtils.getName(dataset.getUrlPath());
                        if (filename != null) {
                            Matcher matcher = regex.matcher(filename);
                            selected = matcher.matches();
                        }
                    } else {
                        selected = true;
                    }

                    if (selected) {
                        filteredDatasets.put(dataset.getID(), new DatasetEntry(dataset, catalogueEntry.getCatalogueUrlBean()));
                    }
                }
            }
        }

        return filteredDatasets;
    }

    // Find where the file will belong after being downloaded
    // Example: s3://bucket/file.nc OR file://directory/file.nc
    public URI getDestinationURI(DatasetEntry datasetEntry) {
        OutputBean output = this.downloadBean.getOutput();

        if (output == null || datasetEntry == null) {
            return null;
        }

        URI uri = output.getDestination();
        if (uri == null) {
            return null;
        }

        Dataset dataset = datasetEntry.getDataset();
        CatalogueUrlBean catalogueUrlBean = datasetEntry.getCatalogueUrlBean();
        String subDir = catalogueUrlBean.getSubDirectory();

        String uriStr = uri.toString();
        if (!uriStr.endsWith("/")) {
            uriStr += "/";
        }
        if (subDir != null && !subDir.isEmpty()) {
            uriStr += subDir + "/";
        }

        String filename = this.getFilename(dataset);
        // The file will be unzipped before upload
        if (ZipUtils.isZipped(filename)) {
            filename = ZipUtils.getUnzippedFilename(filename);
        }
        uriStr += filename;

        try {
            return new URI(uriStr);
        } catch(URISyntaxException ex) {
            LOGGER.error("Invalid URI: " + uriStr, ex);
            return null;
        }
    }

    public String getFilename(Dataset dataset) {
        if (dataset == null) {
            return null;
        }
        String urlPath = dataset.getUrlPath();
        return urlPath == null ? null : FilenameUtils.getName(urlPath);
    }

    public File getDownloadFile(Dataset dataset) {
        OutputBean output = this.downloadBean.getOutput();

        if (output == null || dataset == null) {
            return null;
        }

        File directory = output.getDownloadDir();
        if (directory == null) {
            return null;
        }

        String filename = this.getFilename(dataset);
        return new File(directory, filename);
    }

    private void uploadToS3(File file, AmazonS3URI destinationUri) throws IOException, InterruptedException {
        try (S3Client client = new S3Client()) {
            UploadManager.upload(client, file, destinationUri);
        }
    }

    public static void downloadURIToFile(URI uri, File temporaryFile) throws Exception {
        if (uri == null) {
            throw new IllegalArgumentException("URI is null.");
        }
        if (temporaryFile == null) {
            throw new IllegalArgumentException("The temporaryFile is null.");
        }

        // Check if the temporary file can be created
        if (temporaryFile.exists()) {
            if (!temporaryFile.delete()) {
                throw new IOException(String.format("The temporary file %s already exists and can not be deleted.", temporaryFile));
            }
        } else {
            File directory = temporaryFile.getParentFile();
            if (directory.exists()) {
                if (!directory.isDirectory()) {
                    throw new IOException(String.format("The file %s already exists and is not a directory.", directory));
                }
                if (!directory.canWrite()) {
                    throw new IOException(String.format("The directory %s is not writable.", directory));
                }
            } else {
                if (!directory.mkdirs()) {
                    throw new IOException(String.format("The directory %s doesn't exist and can not be created.", directory));
                }
            }
        }

        String scheme = uri.getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            LOGGER.info(String.format("Copying URL %s to file %s", uri, temporaryFile));
            Files.copy(new File(uri).toPath(), temporaryFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } else {
            NetCDFDownloadManager.downloadHttpURIToFileWithRetry(uri, temporaryFile);
        }
    }

    private static void downloadHttpURIToFileWithRetry(URI uri, File temporaryFile) throws Exception {
        boolean success = false, abort = false;
        int wait = DOWNLOAD_RETRY_INITIAL_WAIT;
        int attempt = 1;
        Exception lastException = null;

        while (!success && !abort) {
            HttpGet httpGet = null;
            try (CloseableHttpClient httpClient = NetCDFDownloadManager.openHttpClient()) {
                httpGet = new HttpGet(uri);
                LOGGER.info(String.format("Downloading URL %s to file %s", uri, temporaryFile));
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    StatusLine httpStatus = response.getStatusLine();
                    if (httpStatus != null) {
                        int statusCode = httpStatus.getStatusCode();
                        if (statusCode < 200 || statusCode >= 300) {
                            throw new IOException(String.format("Error occurred while downloading the URL %s: %s (%d)",
                                    uri, httpStatus.getReasonPhrase(), statusCode));
                        }
                    }

                    // The entity is streamed
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try (InputStream in = entity.getContent(); FileOutputStream out = new FileOutputStream(temporaryFile)) {
                            // The file size may be unknown on the server. This method stop streaming when the file size reach the limit.
                            NetCDFDownloadManager.binaryCopy(in, out, MAX_DOWNLOAD_FILE_SIZE);
                            success = true;
                        }
                    }
                }
            } catch (Exception ex) {
                lastException = ex;
                LOGGER.warn(String.format("Exception occurred while downloading the data file on attempt %d/%d: %s",
                        attempt, MAX_DOWNLOAD_RETRY, ex.getMessage()));

                if (attempt < MAX_DOWNLOAD_RETRY) {
                    LOGGER.warn(String.format("Wait %d seconds before retrying", wait));
                    Thread.sleep(wait * 1000L);

                    // Calculate the wait for the next failed attempt.
                    //   Exponential incremental wait.
                    wait *= 2;
                    attempt++;
                } else {
                    LOGGER.error("Download failed too many times. Abort.");
                    abort = true;
                }
            } finally {
                if (httpGet != null) {
                    // Cancel the connection, if it's still alive
                    httpGet.abort();
                    // Close connections
                    httpGet.reset();
                }
            }
        }

        if (abort && lastException != null) {
            throw lastException;
        }
    }

    public static URI getDatasetFileURI(Dataset dataset) {
        Access httpAccess = dataset.getAccess(ServiceType.HTTPServer);
        if (httpAccess != null) {
            return httpAccess.getStandardUri();
        }

        return null;
    }

    // Open a HTTP Client
    // Java DOC:
    //     http://hc.apache.org/httpcomponents-core-ga/httpcore/apidocs/index.html
    //     http://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/index.html
    // Example: http://hc.apache.org/httpcomponents-client-ga/tutorial/html/fundamentals.html#d5e37
    public static CloseableHttpClient openHttpClient() throws Exception {
        HttpClientBuilder httpClientBuilder = HttpClients.custom();

        // Set HTTP Client timeout
        // NOTE: eReefs GBR1 catalogue is now taking a long time to generate and the request times out.
        //     The timeout needs to be high enough to prevent GBR1 catalogue request from failing.
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(HTTP_CLIENT_TIMEOUT)
                .setConnectionRequestTimeout(HTTP_CLIENT_TIMEOUT)
                .setSocketTimeout(HTTP_CLIENT_TIMEOUT).build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);

        // Try to add support for Self Signed SSL certificates
        // Explicitly set TLS protocols to ensure compatibility with Amazon Linux 2023
        // which disables TLS 1.0 and 1.1 by default in its crypto policy
        SSLContextBuilder selfSignedSSLCertContextBuilder = new SSLContextBuilder();
        selfSignedSSLCertContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        SSLConnectionSocketFactory selfSignedSSLCertSocketFactory = new SSLConnectionSocketFactory(
                selfSignedSSLCertContextBuilder.build(),
                new String[]{"TLSv1.2", "TLSv1.3"},  // Explicitly enable TLS 1.2 and 1.3
                null,  // Use default cipher suites
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());

        httpClientBuilder = httpClientBuilder.setSSLSocketFactory(selfSignedSSLCertSocketFactory);

        return httpClientBuilder.build();
    }

    public static void binaryCopy(InputStream in, OutputStream out) throws IOException {
        binaryCopy(in, out, -1);
    }

    public static void binaryCopy(InputStream in, OutputStream out, long maxBytesFileSize) throws IOException {
        if (in == null || out == null) {
            return;
        }

        long totalBytesRead = 0;

        try {
            byte[] buf = new byte[32 * 1024];  // 32K buffer
            int bytesRead;
            while ((bytesRead = in.read(buf)) != -1) {
                if (maxBytesFileSize >= 0) {
                    totalBytesRead += bytesRead;
                    if (totalBytesRead > maxBytesFileSize) {
                        throw new IOException(String.format(
                            "File size exceeded. The maximum size allowed is %d bytes.", maxBytesFileSize));
                    }
                }
                out.write(buf, 0, bytesRead);
            }
        } finally {
            try {
                out.flush();
            } catch (Exception ex) {
                LOGGER.error(String.format("Cant flush the output: %s", ex.getMessage()), ex);
            }
        }
    }

    protected class CatalogueEntry {
        private Catalog catalogue;
        private CatalogueUrlBean catalogueUrlBean;

        public CatalogueEntry(Catalog catalogue, CatalogueUrlBean catalogueUrlBean) {
            this.catalogue = catalogue;
            this.catalogueUrlBean = catalogueUrlBean;
        }

        public Catalog getCatalogue() {
            return this.catalogue;
        }

        public CatalogueUrlBean getCatalogueUrlBean() {
            return this.catalogueUrlBean;
        }
    }

    protected class DatasetEntry {
        private Dataset dataset;
        private CatalogueUrlBean catalogueUrlBean;

        public DatasetEntry(Dataset dataset, CatalogueUrlBean catalogueUrlBean) {
            this.dataset = dataset;
            this.catalogueUrlBean = catalogueUrlBean;
        }

        public Dataset getDataset() {
            return this.dataset;
        }

        public CatalogueUrlBean getCatalogueUrlBean() {
            return this.catalogueUrlBean;
        }
    }
}
