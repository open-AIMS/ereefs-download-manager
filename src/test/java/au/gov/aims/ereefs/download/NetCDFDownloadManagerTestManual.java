/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.Utils;
import au.gov.aims.ereefs.bean.NetCDFUtils;
import au.gov.aims.ereefs.bean.download.CatalogueUrlBean;
import au.gov.aims.ereefs.bean.download.DownloadBean;
import au.gov.aims.ereefs.bean.metadata.netcdf.NetCDFMetadataBean;
import au.gov.aims.ereefs.database.DatabaseClient;
import au.gov.aims.json.JSONUtils;
import com.amazonaws.services.s3.internal.Constants;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import thredds.client.catalog.Catalog;
import thredds.client.catalog.Dataset;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class NetCDFDownloadManagerTestManual extends DatabaseTestBase {
    private static final Logger LOGGER = Logger.getLogger(NetCDFDownloadManagerTestManual.class);

    /**
     * Verify if a connection interruption would generate an exception or if the
     * Apache HttpClient will attempt to resume the download, resulting in
     * corrupted file.
     * NOTE: It seems like the HttpClient do not attempt to resume downloads.
     */
    @Test
    @Ignore
    public void testConnectionInterrupt() throws Exception {
        // This catalogue contains files smaller than 100MB, which is perfect for this test:
        //     http://dapds00.nci.org.au/thredds/catalog/fx3/gbr1_bgc_924/catalog.html
        URI fileURI = new URI("http://dapds00.nci.org.au/thredds/fileServer/fx3/gbr1_bgc_924/gbr1_bgc_simple_2018-11-19.nc");
        File downloadedFile = File.createTempFile("ereefs-download-manager-test_", "_gbr1_bgc_simple_2018-11-19.nc");
        String md5sum = "2becd9d7dc2e0dfae0c44ff363be9166";

        LOGGER.warn(String.format("Download is starting. Disconnect internet connection momentarily as soon as the file %s is created.",
                downloadedFile));
        NetCDFDownloadManager.downloadURIToFile(fileURI, downloadedFile);

        LOGGER.warn("Download finished. Validating the downloaded file.");
        String errorMessage = NetCDFUtils.scanWithErrorMessage(downloadedFile);
        Assert.assertNull(String.format("The downloaded file is corrupted: %s", downloadedFile), errorMessage);

        String downloadFileMd5sum =  Utils.md5sum(downloadedFile);
        Assert.assertEquals("Unexpected file md5sum", md5sum, downloadFileMd5sum);

        LOGGER.warn(String.format("Download finished. Manually delete the downloaded file:%n    %s.", downloadedFile));
    }

    /**
     * Try to download GBR1 XML Catalogue
     * @throws Exception
     */
    @Test
    @Ignore
    public void testDownloadGBR1Catalogue() throws Exception {
        // Attempt to download a file from eReefs BGC GBR 1k model (those files are about 50 MB only)
        URL catalogueUrl = new URL("http://dapds00.nci.org.au/thredds/catalog/fx3/gbr1_2.0/catalog.xml");
        String downloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream("downloadDefinition/gbr1_v2.json"), true);
        downloadDefinitionStr = downloadDefinitionStr.replace("${CATALOGUE_URL}", catalogueUrl.toString());

        DownloadBean downloadBean = new DownloadBean(new JSONObject(downloadDefinitionStr));

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        List<NetCDFDownloadManager.CatalogueEntry> gbr1Catalogues = downloadManager.getCatalogues();
        Assert.assertNotNull("List of catalogue is null", gbr1Catalogues);
        Assert.assertEquals("List of catalogue count is wrong", 1, gbr1Catalogues.size());
        NetCDFDownloadManager.CatalogueEntry gbr1CatalogueEntry = gbr1Catalogues.get(0);
        Catalog gbr1Catalogue = gbr1CatalogueEntry.getCatalogue();
        Assert.assertNotNull("The catalogue is null. A HTTP error probably occurred.", gbr1Catalogue);

        Map<String, NetCDFDownloadManager.DatasetEntry> datasets = downloadManager.getDatasets();
        Assert.assertTrue("The download manager was able to download the GBR1 catalogue, but it's empty", datasets.size() > 0);

        // Display the dataset IDs in alphabetic order (for manual verification)
        SortedSet<String> sortedDatasetId = new TreeSet<String>(datasets.keySet());
        for (String datasetId : sortedDatasetId) {
            System.out.println(datasetId);
        }
    }

    /**
     * Download a Thredds catalogue, then download a dataset described in the catalogue.
     * IMPORTANT: This test download a XML document AND a ~50 MB NetCDF file from the NCI server.
     *     Add the "Ignore" annotation to skip this test.
     * @throws Exception If something goes wrong
     */
    @Test
    @Ignore
    public void testDownloadADataset() throws Exception {
        // Attempt to download a file from eReefs BGC GBR 1k model (those files are about 50 MB only)
        DownloadBean downloadBean = new DownloadBean(new JSONObject(JSONUtils.streamToString(
                NetCDFDownloadManagerTestManual.class.getClassLoader().getResourceAsStream("downloadDefinition/gbr1_bgc.json"), true)));

        LOGGER.debug(downloadBean.toString());

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        Map<String, NetCDFDownloadManager.DatasetEntry> datasets = downloadManager.getDatasets();

        // Find 2016-03 dataset
        String datasetId = "fx3-gbr1_bgc_924/gbr1_bgc_simple_2018-09-11.nc";
        NetCDFDownloadManager.DatasetEntry datasetEntry_2018_09_11 = datasets.get(datasetId);
        Dataset dataset_2018_09_11 = datasetEntry_2018_09_11.getDataset();
        Assert.assertNotNull("Dataset ID " + datasetId + " is missing", dataset_2018_09_11);

        // Expecting 46 MB (+-10 MB since the file may change in the future)
        long listedFileSize = dataset_2018_09_11.getDataSize() / Constants.MB;
        Assert.assertEquals("Unexpected dataset file size: " + listedFileSize + " MB", 46, listedFileSize, 10);


        // Attempt to download the dataset
        URI fileUri = NetCDFDownloadManager.getDatasetFileURI(dataset_2018_09_11);
        File datasetFile = new File(downloadManager.getDestinationURI(datasetEntry_2018_09_11));
        NetCDFDownloadManager.downloadURIToFile(fileUri, datasetFile);

        Assert.assertNotNull("The dataset download failed (file is null)", datasetFile);
        Assert.assertTrue("The dataset downloaded file doesn't exists: " + datasetFile.getAbsolutePath(), datasetFile.exists());
        // Expecting the same as described in the catalogue (+-1 MB for filesystem overhead and rounding)
        Assert.assertEquals("The dataset file size differ a lot from the listed file size: " + datasetFile.getAbsolutePath(), listedFileSize, datasetFile.length() / Constants.MB, 1);
    }

    @Test
    @Ignore
    public void testScanGBR1File() throws Exception {
        File downloadFile = new File("/home/glafond/Desktop/TMP_INPUT/netcdf/ereefs/gbr1/hydro/hourly/gbr1_simple_2020-09-01.nc");
        String catalogueId = "products__ncaggregate__ereefs__gbr1_2-0__raw";
        String datasetId = "products__ncaggregate__ereefs__gbr1_2-0__raw/gbr1_simple_2020-09-01.nc";
        URI destinationURI = new URI("file:///tmp/gbr1_simple_2020-09-01.nc");
        long newLastModified = downloadFile.lastModified();

        NetCDFMetadataBean newMetadata = NetCDFMetadataBean.create(catalogueId, datasetId, destinationURI, downloadFile, newLastModified);
        Assert.assertNotNull("Could not extract NetCDF file metadata", newMetadata);

        boolean validData = NetCDFUtils.scan(downloadFile);

        Assert.assertTrue("The scan found issues with the downloaded file", validData);
    }

    @Test
    @Ignore
    public void testScanGBR4File() throws Exception {
        File downloadFile = new File("/home/glafond/Desktop/TMP_INPUT/netcdf/ereefs/gbr4_v2/rivers/daily/gbr4_rivers_simple_2020-04.nc");
        String catalogueId = "downloads__ereefs__gbr4_v2-river_tracing-daily";
        String datasetId = "downloads__ereefs__gbr4_v2-river_tracing-daily/gbr4_rivers_simple_2020-04.nc";
        URI destinationURI = new URI("file:///tmp/gbr4_rivers_simple_2020-04.nc");
        long newLastModified = downloadFile.lastModified();

        NetCDFMetadataBean newMetadata = NetCDFMetadataBean.create(catalogueId, datasetId, destinationURI, downloadFile, newLastModified);
        Assert.assertNotNull("Could not extract NetCDF file metadata", newMetadata);

        String errorMessage = NetCDFUtils.scanWithErrorMessage(downloadFile);

        Assert.assertNull("The scan found issues with the downloaded file", errorMessage);
    }

    @Test
    @Ignore
    public void testParseThreddsCatalogUrls() throws Exception {
        String downloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream("downloadDefinition/OceanCurrent_GSLA_NRT00.json"), true);
        downloadDefinitionStr = downloadDefinitionStr.replace("${STORAGE_PROTOCOL}", "file://")
                .replace("${PRIVATE_BUCKET_NAME}", "/tmp/ncanimateTests");

        DownloadBean downloadBean = new DownloadBean(new JSONObject(downloadDefinitionStr));

        LOGGER.debug(downloadBean.toString());

        List<CatalogueUrlBean> catalogueUrlBeans = downloadBean.getCatalogueUrls();
        Assert.assertNotNull("Catalogue list is null", catalogueUrlBeans);
        Assert.assertEquals("Wrong catalogue list size", 3, catalogueUrlBeans.size());

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        Map<String, NetCDFDownloadManager.DatasetEntry> datasetEntryMap = downloadManager.getDatasets();
        Assert.assertNotNull("Dataset map is null", datasetEntryMap);
        Assert.assertEquals("Wrong dataset map size.", 480, datasetEntryMap.size());

        for (NetCDFDownloadManager.DatasetEntry datasetEntry : datasetEntryMap.values()) {
            Dataset dataset = datasetEntry.getDataset();
            URI sourceUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
            URI destinationUri = downloadManager.getDestinationURI(datasetEntry);
            CatalogueUrlBean catalogueUrlBean = datasetEntry.getCatalogueUrlBean();

            String subDirectory = "/" + catalogueUrlBean.getSubDirectory() + "/";
            Assert.assertTrue(String.format("Destination URI do not contain expected sub directory %s. URI: %s",
                    subDirectory, destinationUri),
                    destinationUri.toString().contains(subDirectory));

            System.out.println(String.format("%s => %s", sourceUri, destinationUri));
        }

        DatabaseClient dbClient = this.getDatabaseClient();
        NetCDFDownloadOutput output = downloadManager.download(dbClient, null, false, 1);

        Assert.assertNotNull("The download output is null", output);
        Assert.assertTrue("There is warning(s) in the download output", output.getWarnings().isEmpty());
        Assert.assertTrue("There is error(s) in the download output", output.getErrors().isEmpty());
        Assert.assertFalse("There is no success in the download output", output.getSuccess().isEmpty());

        LOGGER.debug(output.toString());
    }

}
