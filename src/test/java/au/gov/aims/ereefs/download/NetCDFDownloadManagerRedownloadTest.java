/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.bean.download.DownloadBean;
import au.gov.aims.ereefs.bean.metadata.netcdf.NetCDFMetadataBean;
import au.gov.aims.ereefs.database.CacheStrategy;
import au.gov.aims.ereefs.database.DatabaseClient;
import au.gov.aims.ereefs.database.manager.MetadataManager;
import au.gov.aims.ereefs.helper.MetadataHelper;
import au.gov.aims.json.JSONUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import thredds.client.catalog.Dataset;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Map;

public class NetCDFDownloadManagerRedownloadTest extends DatabaseTestBase {
    private static final Logger LOGGER = Logger.getLogger(NetCDFDownloadManagerRedownloadTest.class);

    private static final String ORIG_CATALOGUE = "catalogue/nci_ereefs_gbr4v2_catalog_redownload.xml";
    private static final String NEW_CATALOGUE = "catalogue/nci_ereefs_gbr4v2_catalog_redownload_new.xml";
    private static final String NEW_CATALOGUE_INVALID_ID = "catalogue/nci_ereefs_gbr4v2_catalog_redownload_invalidID.xml";

    private static final String DOWNLOAD_DEFINITION = "downloadDefinition/gbr4_v2_redownload.json";
    private static final String DEFINITION_ID = "downloads/gbr4_v2_redownload";

    /**
     * Test that the DownloadManager can re-download a file when a new version is available,
     * and do not re-download when no new file is available.
     */
    @Test
    public void testRedownload() throws Exception {
        // Get an hold on useful resources
        DatabaseClient dbClient = this.getDatabaseClient();
        MetadataHelper metadataHelper = new MetadataHelper(dbClient, CacheStrategy.DISK);
        metadataHelper.clearCache();


        // Parse original catalogue
        URL origCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(ORIG_CATALOGUE);
        Assert.assertNotNull("The original catalogue XML file could not be found in test resources folder.", origCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String origDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), origDownloadDefinitionStr);
        origDownloadDefinitionStr = origDownloadDefinitionStr.replace("${CATALOGUE_URL}", origCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean origDownloadBean = new DownloadBean(new JSONObject(origDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, origDownloadBean.getId());


        // Parse updated catalogue
        URL newCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(NEW_CATALOGUE);
        Assert.assertNotNull("The new catalogue XML file could not be found in test resources folder.", newCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String newDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), newDownloadDefinitionStr);
        newDownloadDefinitionStr = newDownloadDefinitionStr.replace("${CATALOGUE_URL}", newCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean newDownloadBean = new DownloadBean(new JSONObject(newDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, newDownloadBean.getId());

        long downloadTimestamp = -1,
            reDownloadTimestamp = -1,
            gbr4_simple_2018_10_lastmodified = -1,
            gbr4_simple_2018_11_lastmodified = -1,
            gbr4_simple_2018_12_lastmodified = -1,
            gbr4_simple_2019_01_lastmodified = -1;


        // 1. Download the files

        {
            NetCDFDownloadManagerRedownloadTest.copyOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager origDownloadManager = new NetCDFDownloadManager(origDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> origDatasetEntryMap = origDownloadManager.getDatasets();
            Assert.assertNotNull("Original datasets is null", origDatasetEntryMap);

            // Ensure nothing have been downloaded yet
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = origDownloadManager.getDownloadFile(dataset);
                URI destinationURI = origDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);
                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                Assert.assertFalse(String.format("The destination file already exists: %s", destinationFile),
                        destinationFile.exists());
            }
            Assert.assertEquals(String.format("Wrong number of file in original catalogue: %s", ORIG_CATALOGUE),
                    4, origDatasetEntryMap.size());

            // Ensure the files are not in the DB yet
            int metadataCount = 0;
            for (NetCDFMetadataBean metadata : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                LOGGER.error(String.format("Unexpected dataset: %s", metadata.getId()));
                metadataCount++;
            }
            Assert.assertEquals("The DB contains dataset before the download has started", 0, metadataCount);

            // Download the files
            downloadTimestamp = new Date().getTime();
            origDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = origDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(origDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                        destinationFile.exists());

                switch(destinationFile.getName()) {
                    case "gbr4_simple_2018-10.nc":
                        gbr4_simple_2018_10_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-11.nc":
                        gbr4_simple_2018_11_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-12.nc":
                        gbr4_simple_2018_12_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2019-01.nc":
                        gbr4_simple_2019_01_lastmodified = destinationFile.lastModified();
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:ac18606715798395128a8cd1dde88712", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-10T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-19T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 4, metadataCount);
        }


        // 1sec pause to ensure file dates changes if they are re-downloaded
        Thread.sleep(1000);


        // 2. Trigger the re-download (updated catalogue)

        {
            // Update source files
            NetCDFDownloadManagerRedownloadTest.updateOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager newDownloadManager = new NetCDFDownloadManager(newDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> newDatasetEntryMap = newDownloadManager.getDatasets();
            Assert.assertNotNull("New datasets is null", newDatasetEntryMap);

            // Ensure the state before the download is as expected
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = newDownloadManager.getDownloadFile(dataset);
                URI destinationURI = newDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The new original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The new original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);

                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                if ("gbr4_simple_2019-02.nc".equals(destinationFile.getName())) {
                    Assert.assertFalse(String.format("The new destination file already exists: %s", destinationFile),
                            destinationFile.exists());
                } else {
                    Assert.assertTrue(String.format("The old destination file does not exists: %s", destinationFile),
                            destinationFile.exists());
                }
            }
            Assert.assertEquals(String.format("Wrong number of file in new catalogue: %s", NEW_CATALOGUE),
                    5, newDatasetEntryMap.size());

            // Re-download the files
            reDownloadTimestamp = new Date().getTime();
            newDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = newDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(newDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                        destinationFile.exists());

                switch(destinationFile.getName()) {
                    // File haven't changed
                    case "gbr4_simple_2018-10.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_10_lastmodified, destinationFile.lastModified());
                        break;

                    // File haven't changed
                    case "gbr4_simple_2018-11.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_11_lastmodified, destinationFile.lastModified());
                        break;

                    // New file, different checksum. Expected re-upload
                    case "gbr4_simple_2018-12.nc":
                        Assert.assertTrue(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_12_lastmodified < destinationFile.lastModified());
                        break;

                    // New file, same checksum. File expected to be left unchanged
                    case "gbr4_simple_2019-01.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2019_01_lastmodified, destinationFile.lastModified());
                        break;

                    // New file
                    case "gbr4_simple_2019-02.nc":
                        Assert.assertTrue(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2019_01_lastmodified < destinationFile.lastModified());
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            int metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:af2fd0afefa1a7630a91b29dbae87056", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-08T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-20T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-02_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:3ac740ab7769a91d6160bbb55a804bef", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-20T02:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reDownloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 5, metadataCount);
        }
    }



    /**
     * Test that the DownloadManager won't re-download a file which have invalid ID in the DB.
     */
    @Test
    public void testNotRedownloadInvalidId() throws Exception {
        // Get an hold on useful resources
        DatabaseClient dbClient = this.getDatabaseClient();
        MetadataManager metadataManager = new MetadataManager(dbClient, CacheStrategy.DISK);
        MetadataHelper metadataHelper = new MetadataHelper(dbClient, CacheStrategy.DISK);
        metadataHelper.clearCache();


        // Parse original catalogue
        URL origCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(ORIG_CATALOGUE);
        Assert.assertNotNull("The original catalogue XML file could not be found in test resources folder.", origCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String origDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), origDownloadDefinitionStr);
        origDownloadDefinitionStr = origDownloadDefinitionStr.replace("${CATALOGUE_URL}", origCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean origDownloadBean = new DownloadBean(new JSONObject(origDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, origDownloadBean.getId());

        // Parse updated catalogue
        URL newCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(NEW_CATALOGUE_INVALID_ID);
        Assert.assertNotNull("The new catalogue XML file could not be found in test resources folder.", newCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String newDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), newDownloadDefinitionStr);
        newDownloadDefinitionStr = newDownloadDefinitionStr.replace("${CATALOGUE_URL}", newCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean newDownloadBean = new DownloadBean(new JSONObject(newDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, newDownloadBean.getId());

        long downloadTimestamp = -1,
            reDownloadTimestamp = -1,
            gbr4_simple_2018_10_lastmodified = -1,
            gbr4_simple_2018_11_lastmodified = -1,
            gbr4_simple_2018_12_lastmodified = -1,
            gbr4_simple_2019_01_lastmodified = -1;


        // 1. Download the files

        {
            NetCDFDownloadManagerRedownloadTest.copyOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager origDownloadManager = new NetCDFDownloadManager(origDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> origDatasetEntryMap = origDownloadManager.getDatasets();
            Assert.assertNotNull("Original datasets is null", origDatasetEntryMap);

            // Ensure nothing have been downloaded yet
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = origDownloadManager.getDownloadFile(dataset);
                URI destinationURI = origDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);
                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                Assert.assertFalse(String.format("The destination file already exists: %s", destinationFile),
                        destinationFile.exists());
            }
            Assert.assertEquals(String.format("Wrong number of file in original catalogue: %s", ORIG_CATALOGUE),
                    4, origDatasetEntryMap.size());

            // Ensure the files are not in the DB yet
            int metadataCount = 0;
            for (NetCDFMetadataBean metadata : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                LOGGER.error(String.format("Unexpected dataset: %s", metadata.getId()));
                metadataCount++;
            }
            Assert.assertEquals("The DB contains dataset before the download has started", 0, metadataCount);

            // Download the files
            downloadTimestamp = new Date().getTime();
            origDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = origDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(origDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                        destinationFile.exists());

                switch(destinationFile.getName()) {
                    case "gbr4_simple_2018-10.nc":
                        gbr4_simple_2018_10_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-11.nc":
                        gbr4_simple_2018_11_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-12.nc":
                        gbr4_simple_2018_12_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2019-01.nc":
                        gbr4_simple_2019_01_lastmodified = destinationFile.lastModified();
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }


            // Update the ID and remove lastDownloaded of some datasets
            // so they represent old record with "invalid" IDs
            JSONObject gbr4_simple_2018_10_nc =
                    metadataHelper.getNetCDFMetadata(DEFINITION_ID, "fx3-gbr4_v2/gbr4_simple_2018-10_nc").toJSON();

            metadataManager.delete("downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc");
            gbr4_simple_2018_10_nc.put("_id", "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10.nc");
            gbr4_simple_2018_10_nc.remove("lastDownloaded");
            metadataManager.getTable().insert(gbr4_simple_2018_10_nc, false);

            JSONObject gbr4_simple_2018_11_nc =
                    metadataHelper.getNetCDFMetadata(DEFINITION_ID, "fx3-gbr4_v2/gbr4_simple_2018-11_nc").toJSON();

            metadataManager.delete("downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc");
            gbr4_simple_2018_11_nc.put("_id", "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11.nc");
            gbr4_simple_2018_11_nc.remove("lastDownloaded");
            metadataManager.getTable().insert(gbr4_simple_2018_11_nc, false);

            metadataHelper.clearCache();
            metadataManager.clearCache();


            // Check download file checksum, status, lastModified and lastDownloaded
            metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();

                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10.nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertEquals(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastModified(), metadataBean.getLastDownloaded());
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11.nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertEquals(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastModified(), metadataBean.getLastDownloaded());
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:ac18606715798395128a8cd1dde88712", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-10T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-19T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 4, metadataCount);
        }


        // 1sec pause to ensure file dates changes if they are re-downloaded
        Thread.sleep(1000);


        // 2. Trigger the re-download (updated catalogue)

        {
            // Update source files
            NetCDFDownloadManagerRedownloadTest.updateOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager newDownloadManager = new NetCDFDownloadManager(newDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> newDatasetEntryMap = newDownloadManager.getDatasets();
            Assert.assertNotNull("New datasets is null", newDatasetEntryMap);

            // Ensure the state before the download is as expected
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = newDownloadManager.getDownloadFile(dataset);
                URI destinationURI = newDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The new original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The new original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);

                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                if ("gbr4_simple_2019-02.nc".equals(destinationFile.getName())) {
                    Assert.assertFalse(String.format("The new destination file already exists: %s", destinationFile),
                            destinationFile.exists());
                } else {
                    Assert.assertTrue(String.format("The old destination file does not exists: %s", destinationFile),
                            destinationFile.exists());
                }
            }
            Assert.assertEquals(String.format("Wrong number of file in catalogue: %s", ORIG_CATALOGUE),
                    4, newDatasetEntryMap.size());

            // Re-download the files
            reDownloadTimestamp = new Date().getTime();
            newDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = newDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(newDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                        destinationFile.exists());

                switch(destinationFile.getName()) {
                    // File haven't changed
                    case "gbr4_simple_2018-10.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_10_lastmodified, destinationFile.lastModified());
                        break;

                    // Last modified has changed in catalogue, but the file checksum is the same.
                    // Re-upload not expected (destination file unchanged)
                    case "gbr4_simple_2018-11.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_11_lastmodified, destinationFile.lastModified());
                        break;

                    // File haven't changed
                    case "gbr4_simple_2018-12.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_12_lastmodified, destinationFile.lastModified());
                        break;

                    // File haven't changed
                    case "gbr4_simple_2019-01.nc":
                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2019_01_lastmodified, destinationFile.lastModified());
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            int metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10.nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertEquals(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastModified(), metadataBean.getLastDownloaded());
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11.nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:ac18606715798395128a8cd1dde88712", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-10T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-19T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 4, metadataCount);
        }
    }




    /**
     * Test that deleted files are flagged as DELETED by the download manager,
     * flagged files are not re-downloaded, unless new version is available.
     */
    @Test
    public void testRedownloadDeleted() throws Exception {
        // Get an hold on useful resources
        DatabaseClient dbClient = this.getDatabaseClient();
        MetadataHelper metadataHelper = new MetadataHelper(dbClient, CacheStrategy.DISK);
        metadataHelper.clearCache();


        // Parse original catalogue
        URL origCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(ORIG_CATALOGUE);
        Assert.assertNotNull("The original catalogue XML file could not be found in test resources folder.", origCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String origDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), origDownloadDefinitionStr);
        origDownloadDefinitionStr = origDownloadDefinitionStr.replace("${CATALOGUE_URL}", origCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean origDownloadBean = new DownloadBean(new JSONObject(origDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, origDownloadBean.getId());


        // Parse updated catalogue
        URL newCatalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource(NEW_CATALOGUE);
        Assert.assertNotNull("The new catalogue XML file could not be found in test resources folder.", newCatalogueUrl);

        // Load the download definition (for the original catalogue)
        String newDownloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream(DOWNLOAD_DEFINITION), true);
        Assert.assertNotNull(String.format("Can get download definition file: %s", DOWNLOAD_DEFINITION), newDownloadDefinitionStr);
        newDownloadDefinitionStr = newDownloadDefinitionStr.replace("${CATALOGUE_URL}", newCatalogueUrl.toString());

        // Parse the download definition
        DownloadBean newDownloadBean = new DownloadBean(new JSONObject(newDownloadDefinitionStr));
        Assert.assertEquals("Wrong definition ID", DEFINITION_ID, newDownloadBean.getId());

        long downloadTimestamp = -1,
            reDownloadTimestamp = -1,
            reReDownloadTimestamp = -1,
            gbr4_simple_2018_10_lastmodified = -1,
            gbr4_simple_2018_11_lastmodified = -1,
            gbr4_simple_2018_12_lastmodified = -1,
            gbr4_simple_2019_01_lastmodified = -1;


        // 1. Download the files

        {
            NetCDFDownloadManagerRedownloadTest.copyOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager origDownloadManager = new NetCDFDownloadManager(origDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> origDatasetEntryMap = origDownloadManager.getDatasets();
            Assert.assertNotNull("Original datasets is null", origDatasetEntryMap);

            // Ensure nothing have been downloaded yet
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = origDownloadManager.getDownloadFile(dataset);
                URI destinationURI = origDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);
                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                Assert.assertFalse(String.format("The destination file already exists: %s", destinationFile),
                        destinationFile.exists());
            }
            Assert.assertEquals(String.format("Wrong number of file in original catalogue: %s", ORIG_CATALOGUE),
                    4, origDatasetEntryMap.size());

            // Ensure the files are not in the DB yet
            int metadataCount = 0;
            for (NetCDFMetadataBean metadata : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                LOGGER.error(String.format("Unexpected dataset: %s", metadata.getId()));
                metadataCount++;
            }
            Assert.assertEquals("The DB contains dataset before the download has started", 0, metadataCount);

            // Download the files
            downloadTimestamp = new Date().getTime();
            origDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = origDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(origDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                        destinationFile.exists());

                switch(destinationFile.getName()) {
                    case "gbr4_simple_2018-10.nc":
                        gbr4_simple_2018_10_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-11.nc":
                        gbr4_simple_2018_11_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2018-12.nc":
                        gbr4_simple_2018_12_lastmodified = destinationFile.lastModified();
                        break;

                    case "gbr4_simple_2019-01.nc":
                        gbr4_simple_2019_01_lastmodified = destinationFile.lastModified();
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                "MD5:ac18606715798395128a8cd1dde88712", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-10T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-19T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 4, metadataCount);
        }


        // 1sec pause to ensure file dates changes if they are re-downloaded
        Thread.sleep(1000);


        // 2. Re-trigger the same download after some files have been deleted

        {
            // Delete some destination files

            // Do not delete gbr4_simple_2018-10.nc

            // This file is old and its source has not changed. Should NOT be re-downloaded. Should be flag as DELETED
            File gbr4_simple_2018_11 = new File("/tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_2018-11.nc");
            Assert.assertTrue(String.format("Could not delete downloaded file: %s", gbr4_simple_2018_11),
                    gbr4_simple_2018_11.delete());

            // This file source have been updated, therefore it should be re-downloaded
            File gbr4_simple_2018_12 = new File("/tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_2018-12.nc");
            Assert.assertTrue(String.format("Could not delete downloaded file: %s", gbr4_simple_2018_12),
                    gbr4_simple_2018_12.delete());

            // This file "last modified" was updated, but the checksum is the same
            File gbr4_simple_2019_01 = new File("/tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_2019-01.nc");
            Assert.assertTrue(String.format("Could not delete downloaded file: %s", gbr4_simple_2019_01),
                    gbr4_simple_2019_01.delete());

            // Create a download manager for the download definition
            NetCDFDownloadManager origDownloadManager = new NetCDFDownloadManager(origDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> origDatasetEntryMap = origDownloadManager.getDatasets();
            Assert.assertNotNull("Original datasets is null", origDatasetEntryMap);

            // Ensure the state before the download is as expected
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = origDownloadManager.getDownloadFile(dataset);
                URI destinationURI = origDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);
                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                if ("gbr4_simple_2018-10.nc".equals(destinationFile.getName())) {
                    Assert.assertTrue(String.format("The destination file was deleted: %s", destinationFile),
                            destinationFile.exists());
                } else {
                    Assert.assertFalse(String.format("The destination file was re-downloaded: %s", destinationFile),
                            destinationFile.exists());
                }
            }
            Assert.assertEquals(String.format("Wrong number of file in original catalogue: %s", ORIG_CATALOGUE),
                    4, origDatasetEntryMap.size());

            // Download the files
            reDownloadTimestamp = new Date().getTime();
            origDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : origDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = origDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(origDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                if ("gbr4_simple_2018-10.nc".equals(destinationFile.getName())) {
                    Assert.assertTrue(String.format("The destination file was deleted: %s", destinationFile),
                            destinationFile.exists());
                } else {
                    Assert.assertFalse(String.format("The new destination file already exists: %s", destinationFile),
                            destinationFile.exists());
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            int metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.DELETED, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:ac18606715798395128a8cd1dde88712", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.DELETED, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-10T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.DELETED, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-19T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 4, metadataCount);
        }


        // 1sec pause to ensure file dates changes if they are re-downloaded
        Thread.sleep(1000);


        // 3. Trigger the re-download (updated catalogue)


        {
            // Update source files
            NetCDFDownloadManagerRedownloadTest.updateOriginalNetCDFFiles();

            // Create a download manager for the download definition
            NetCDFDownloadManager newDownloadManager = new NetCDFDownloadManager(newDownloadBean);

            // Get datasets from the catalogue
            Map<String, NetCDFDownloadManager.DatasetEntry> newDatasetEntryMap = newDownloadManager.getDatasets();
            Assert.assertNotNull("New datasets is null", newDatasetEntryMap);

            // Ensure the state before the download is as expected
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
                File tempDownloadFile = newDownloadManager.getDownloadFile(dataset);
                URI destinationURI = newDownloadManager.getDestinationURI(datasetEntry);

                Assert.assertNotNull(String.format("The new original file is null for dataset: %s", dataset.getID()),
                        sourceUri);
                Assert.assertTrue(String.format("The new original file does not exist: %s", sourceUri),
                        new File(sourceUri).exists());

                Assert.assertFalse(String.format("The file was found in its temporary destination: %s", tempDownloadFile),
                        tempDownloadFile.exists());
                Assert.assertNotNull("Destination URI is null", destinationURI);

                Assert.assertTrue(String.format("Unexpected destination URI: %s", destinationURI),
                        destinationURI.toString().startsWith("file:///tmp/downloadedNetcdfFiles/gbr4_v2/gbr4_simple_"));

                File destinationFile = new File(destinationURI);
                if ("gbr4_simple_2018-10.nc".equals(destinationFile.getName())) {
                    Assert.assertTrue(String.format("The destination file was deleted: %s", destinationFile),
                            destinationFile.exists());
                } else {
                    Assert.assertFalse(String.format("The new destination file already exists: %s", destinationFile),
                            destinationFile.exists());
                }
            }
            Assert.assertEquals(String.format("Wrong number of file in new catalogue: %s", NEW_CATALOGUE),
                    5, newDatasetEntryMap.size());


            // Re-download the files
            reReDownloadTimestamp = new Date().getTime();
            newDownloadManager.download(dbClient, null, false);

            // Check that all the files have been downloaded
            for (NetCDFDownloadManager.DatasetEntry datasetEntry : newDatasetEntryMap.values()) {
                Dataset dataset = datasetEntry.getDataset();
                File downloadFile = newDownloadManager.getDownloadFile(dataset);
                File destinationFile = new File(newDownloadManager.getDestinationURI(datasetEntry));

                Assert.assertFalse(String.format("The file is still in its temporary location: %s", downloadFile),
                        downloadFile.exists());

                switch(destinationFile.getName()) {
                    // File haven't changed - expect not deleted
                    case "gbr4_simple_2018-10.nc":
                        Assert.assertTrue(String.format("The file was deleted from its final location: %s", destinationFile),
                                destinationFile.exists());

                        Assert.assertEquals(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_10_lastmodified, destinationFile.lastModified());
                        break;

                    // File haven't changed - expect still deleted
                    case "gbr4_simple_2018-11.nc":
                        Assert.assertFalse(String.format("The file was re-downloaded (copied) to its final location: %s", destinationFile),
                                destinationFile.exists());
                        break;

                    // New file, different checksum. Expected re-upload
                    case "gbr4_simple_2018-12.nc":
                        Assert.assertTrue(String.format("The file was not re-downloaded (copied) to its final location: %s", destinationFile),
                                destinationFile.exists());

                        Assert.assertTrue(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2018_12_lastmodified < destinationFile.lastModified());
                        break;

                    // New file, same checksum. File expected to not be re-downloaded
                    case "gbr4_simple_2019-01.nc":
                        Assert.assertFalse(String.format("The file have been re-downloaded (copied) to its final location: %s", destinationFile),
                                destinationFile.exists());
                        break;

                    // New file
                    case "gbr4_simple_2019-02.nc":
                        Assert.assertTrue(String.format("The file was not downloaded (copied) to its final location: %s", destinationFile),
                                destinationFile.exists());

                        Assert.assertTrue(String.format("Wrong lastmodified for: %s", destinationFile.getName()),
                                gbr4_simple_2019_01_lastmodified < destinationFile.lastModified());
                        break;

                    default:
                        Assert.fail(String.format("Unexpected downloaded file: %s", destinationFile));
                }
            }

            // Check download file checksum, status, lastModified and lastDownloaded
            int metadataCount = 0;
            for (NetCDFMetadataBean metadataBean : metadataHelper.getAllNetCDFMetadatas(DEFINITION_ID)) {
                String datasetId = metadataBean.getId();
                switch(datasetId) {
                    // Unchanged - remains valid
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-10_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:b16fa142ee09acd1ddb9d06f49d0d21a", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-11-05T12:46:10.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    // Unchanged - remains deleted
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-11_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:5b2e920be900804bf2d7b415f3aa60fa", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.DELETED, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2018-12-02T14:05:34.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= downloadTimestamp &&
                                metadataBean.getLastDownloaded() < reDownloadTimestamp);
                        break;

                    // Changed - new checksum
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2018-12_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:af2fd0afefa1a7630a91b29dbae87056", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-08T08:52:59.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reReDownloadTimestamp);
                        break;

                    // Updated "last modified" but same checksum
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-01_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:927f8425be1d7247f6d5a0e8ccb040f8", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.DELETED, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-20T01:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reReDownloadTimestamp);
                        break;

                    // New file
                    case "downloads/gbr4_v2_redownload/fx3-gbr4_v2/gbr4_simple_2019-02_nc":
                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                            "MD5:3ac740ab7769a91d6160bbb55a804bef", metadataBean.getChecksum());

                        Assert.assertEquals(String.format("Wrong checksum for dataset ID %s", datasetId),
                                NetCDFMetadataBean.Status.VALID, metadataBean.getStatus());

                        Assert.assertEquals(String.format("Wrong lastModified for dataset ID %s", datasetId),
                                "2019-01-20T02:09:58.000Z", new DateTime(metadataBean.getLastModified(), DateTimeZone.UTC).toString());

                        Assert.assertTrue(String.format("Wrong lastDownloaded for dataset ID %s", datasetId),
                                metadataBean.getLastDownloaded() >= reReDownloadTimestamp);
                        break;

                    default:
                        Assert.fail(String.format("Unexpected dataset found in the DB: %s", datasetId));
                }
                metadataCount++;
            }
            Assert.assertEquals("Wrong number of dataset in DB after the download", 5, metadataCount);
        }
    }


    private static void copyOriginalNetCDFFiles() throws Exception {
        // Delete download directory to start fresh
        File downloadDir = new File("/tmp/downloadedNetcdfFiles");
        FileUtils.deleteDirectory(downloadDir);

        // Copy input files to the original folder as defined in HTTPServer service of the catalogue
        File origFileDir = new File("/tmp/originalNetcdfFiles/gbr4_v2/thredds/fileServer/");
        File origGBR4FileDir = new File(origFileDir, "fx3/gbr4_v2");
        origGBR4FileDir.mkdirs();

        URL gbr4_simple_2018_10_resource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2018-10.nc");
        URL gbr4_simple_2018_11_resource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2018-11.nc");
        URL gbr4_simple_2018_12_resource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2018-12.nc");
        URL gbr4_simple_2019_01_resource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2019-01.nc");
        File gbr4_simple_2018_10_file = new File(origGBR4FileDir, "gbr4_simple_2018-10.nc");
        File gbr4_simple_2018_11_file = new File(origGBR4FileDir, "gbr4_simple_2018-11.nc");
        File gbr4_simple_2018_12_file = new File(origGBR4FileDir, "gbr4_simple_2018-12.nc");
        File gbr4_simple_2019_01_file = new File(origGBR4FileDir, "gbr4_simple_2019-01.nc");

        Files.copy(new File(gbr4_simple_2018_10_resource.toURI()).toPath(),
                gbr4_simple_2018_10_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2018_11_resource.toURI()).toPath(),
                gbr4_simple_2018_11_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2018_12_resource.toURI()).toPath(),
                gbr4_simple_2018_12_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2019_01_resource.toURI()).toPath(),
                gbr4_simple_2019_01_file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private static void updateOriginalNetCDFFiles() throws Exception {
        // Copy input files to the original folder as defined in HTTPServer service of the catalogue
        File origFileDir = new File("/tmp/originalNetcdfFiles/gbr4_v2/thredds/fileServer/");
        File origGBR4FileDir = new File(origFileDir, "fx3/gbr4_v2");
        origGBR4FileDir.mkdirs();

        URL gbr4_simple_2018_11_ressource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2018-11.nc");
        URL gbr4_simple_2018_12_modified_ressource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2018-12_modified.nc");
        URL gbr4_simple_2019_01_ressource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2019-01.nc");
        URL gbr4_simple_2019_02_ressource = NetCDFDownloadManagerTest.class.getClassLoader().getResource("netcdf/gbr4_simple_2019-02.nc");
        File gbr4_simple_2018_11_file = new File(origGBR4FileDir, "gbr4_simple_2018-11.nc");
        File gbr4_simple_2018_12_file = new File(origGBR4FileDir, "gbr4_simple_2018-12.nc");
        File gbr4_simple_2019_01_file = new File(origGBR4FileDir, "gbr4_simple_2019-01.nc");
        File gbr4_simple_2019_02_file = new File(origGBR4FileDir, "gbr4_simple_2019-02.nc");

        Files.copy(new File(gbr4_simple_2018_11_ressource.toURI()).toPath(),
                gbr4_simple_2018_11_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2018_12_modified_ressource.toURI()).toPath(),
                gbr4_simple_2018_12_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2019_01_ressource.toURI()).toPath(),
                gbr4_simple_2019_01_file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        Files.copy(new File(gbr4_simple_2019_02_ressource.toURI()).toPath(),
                gbr4_simple_2019_02_file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
}
