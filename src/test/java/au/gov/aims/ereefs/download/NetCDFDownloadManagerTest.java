/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.bean.download.CatalogueUrlBean;
import au.gov.aims.ereefs.bean.download.DownloadBean;
import au.gov.aims.json.JSONUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import thredds.client.catalog.Catalog;
import thredds.client.catalog.Dataset;
import thredds.client.catalog.Service;
import ucar.nc2.units.DateType;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetCDFDownloadManagerTest {
    private static final Logger LOGGER = Logger.getLogger(NetCDFDownloadManagerTest.class);

    @Test
    public void testDryRunDownloadAllDatasets() throws Exception {
        // Attempt to download a file from eReefs BGC GBR 1k model (those files are about 50 MB only)
        URL catalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource("catalogue/nci_ereefs_gbr1v2_catalog.xml");
        Assert.assertNotNull("The catalogue XML file could not be found in test resources folder.", catalogueUrl);

        String downloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream("downloadDefinition/gbr1_v2.json"), true);
        downloadDefinitionStr = downloadDefinitionStr.replace("${CATALOGUE_URL}", catalogueUrl.toString());

        DownloadBean downloadBean = new DownloadBean(new JSONObject(downloadDefinitionStr));

        LOGGER.debug(downloadBean.toString());

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        Map<String, NetCDFDownloadManager.DatasetEntry> datasets = downloadManager.getDatasets();
        Assert.assertNotNull("Datasets is null", datasets);

        for (NetCDFDownloadManager.DatasetEntry datasetEntry : datasets.values()) {
            Dataset dataset = datasetEntry.getDataset();
            File downloadFile = downloadManager.getDownloadFile(dataset);
            URI destinationURI = downloadManager.getDestinationURI(datasetEntry);

            Assert.assertFalse("The file was downloaded: " + downloadFile, downloadFile.exists());
            Assert.assertNotNull("Destination URI is null", destinationURI);
            Assert.assertTrue("Unexpected destination URI: " + destinationURI, destinationURI.toString().startsWith("s3://netcdf/gbr1_v2/gbr1_simple_"));
        }

        Assert.assertEquals(1509, datasets.size());
    }


    /**
     * This test simply verify that the Thredds catalogue parser is working as expected.
     * This should only cause issue after upgrading the netcdf4 dependency.
     * NOTE: This test is not necessary. I wrote it while developing the app. It doesn't hurt to keep it here.
     * @throws Exception If something goes wrong
     */
    @Test
    public void testParseThreddsCatalogUrl() throws Exception {
        //URL catalogueUrl = new URL("http://dapds00.nci.org.au/thredds/catalog/fx3/gbr4_v2/catalog.xml");
        URL catalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource("catalogue/nci_ereefs_gbr4v2_catalog.xml");
        Assert.assertNotNull("The catalogue XML file could not be found in test resources folder.", catalogueUrl);

        String downloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream("downloadDefinition/gbr4_v2.json"), true);
        downloadDefinitionStr = downloadDefinitionStr.replace("${CATALOGUE_URL}", catalogueUrl.toString());

        DownloadBean downloadBean = new DownloadBean(new JSONObject(downloadDefinitionStr));

        LOGGER.debug(downloadBean.toString());

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        List<NetCDFDownloadManager.CatalogueEntry> catalogueEntries = downloadManager.getCatalogues();
        Assert.assertNotNull("List of catalogue is null", catalogueEntries);
        Assert.assertEquals("List of catalogue count is wrong", 1, catalogueEntries.size());
        NetCDFDownloadManager.CatalogueEntry catalogueEntry = catalogueEntries.get(0);
        Catalog catalogue = catalogueEntry.getCatalogue();
        Assert.assertNotNull("Catalogue is null", catalogue);

        // Check services
        List<Service> services = catalogue.getServices();
        Map<String, Integer> serviceCountMap = new HashMap<String, Integer>();
        for (Service service : services) {
            String serviceName = service.getName();
            int serviceCount = serviceCountMap.containsKey(serviceName) ? serviceCountMap.get(serviceName) + 1 : 1;
            serviceCountMap.put(serviceName, serviceCount);

            List<Service> nestedServices = service.getNestedServices();
            if (nestedServices != null && !nestedServices.isEmpty()) {
                for (Service nestedService : nestedServices) {
                    String nestedServiceName = nestedService.getName();
                    int nestedServiceCount = serviceCountMap.containsKey(nestedServiceName) ? serviceCountMap.get(nestedServiceName) + 1 : 1;
                    serviceCountMap.put(nestedServiceName, nestedServiceCount);
                }
            }
        }

        // Base services
        Assert.assertTrue("Missing service 'all'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'all'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'latest'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'latest'", new Integer(1), serviceCountMap.get("all"));

        // Nested services
        Assert.assertTrue("Missing service 'ncdods'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'ncdods'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'HTTPServer'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'HTTPServer'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'wms'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'wms'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'wcs'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'wcs'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'subsetServer'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'subsetServer'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'ncml'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'ncml'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'uddc'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'uddc'", new Integer(1), serviceCountMap.get("all"));

        Assert.assertTrue("Missing service 'iso'", serviceCountMap.containsKey("all"));
        Assert.assertEquals("Missing service 'iso'", new Integer(1), serviceCountMap.get("all"));

        // Ensure no unexpected services
        Assert.assertEquals("Wrong number of services", 10, serviceCountMap.size());

        // Get all datasets, recursively
        Map<String, NetCDFDownloadManager.DatasetEntry> datasetEntryMap = downloadManager.getDatasets();

        // Check for 3 datasets (we won't check them all)
        boolean found_2019_01 = false;
        boolean found_2016_03 = false;
        boolean found_2010_09 = false;
        for (NetCDFDownloadManager.DatasetEntry datasetEntry : datasetEntryMap.values()) {
            Dataset dataset = datasetEntry.getDataset();
            String filename = FilenameUtils.getName(dataset.getUrlPath());
            URI datasetUri = NetCDFDownloadManager.getDatasetFileURI(dataset);
            DateType lastModified = dataset.getLastModifiedDate();

            LOGGER.debug(String.format("DS name: '%s' ID: '%s' Path: '%s' Filename: '%s' Modified: '%s' URI: '%s'",
                    dataset.getName(),
                    dataset.getID(),
                    dataset.getUrlPath(),
                    filename,
                    lastModified,
                    datasetUri));

            if ("gbr4_simple_2019-01.nc".equals(filename)) {
                Assert.assertEquals("eReefs GBR4 SHOC Model v2.0 Results for 2019-01", dataset.getName());
                Assert.assertEquals("fx3-gbr4_v2/gbr4_simple_2019-01.nc", dataset.getID());
                Assert.assertEquals("fx3/gbr4_v2/gbr4_simple_2019-01.nc", dataset.getUrlPath());
                Assert.assertEquals("2019-01-19T01:09:58Z", lastModified.toString());
                Assert.assertNotNull("Dataset URI is null", datasetUri);
                Assert.assertTrue("Wrong dataset URI: " + datasetUri, datasetUri.toString().endsWith("/thredds/fileServer/fx3/gbr4_v2/gbr4_simple_2019-01.nc"));
                found_2019_01 = true;

            } else if ("gbr4_simple_2016-03.nc".equals(filename)) {
                Assert.assertEquals("eReefs GBR4 SHOC Model v2.0 Results for 2016-03", dataset.getName());
                Assert.assertEquals("fx3-gbr4_v2/gbr4_simple_2016-03.nc", dataset.getID());
                Assert.assertEquals("fx3/gbr4_v2/gbr4_simple_2016-03.nc", dataset.getUrlPath());
                Assert.assertEquals("2016-06-22T08:52:22Z", lastModified.toString());
                Assert.assertNotNull("Dataset URI is null", datasetUri);
                Assert.assertTrue("Wrong dataset URI: " + datasetUri, datasetUri.toString().endsWith("/thredds/fileServer/fx3/gbr4_v2/gbr4_simple_2016-03.nc"));
                found_2016_03 = true;

            } else if ("gbr4_simple_2010-09.nc".equals(filename)) {
                Assert.assertEquals("eReefs GBR4 SHOC Model v2.0 Results for 2010-09", dataset.getName());
                Assert.assertEquals("fx3-gbr4_v2/gbr4_simple_2010-09.nc", dataset.getID());
                Assert.assertEquals("fx3/gbr4_v2/gbr4_simple_2010-09.nc", dataset.getUrlPath());
                Assert.assertEquals("2016-06-22T01:34:49Z", lastModified.toString());
                Assert.assertNotNull("Dataset URI is null", datasetUri);
                Assert.assertTrue("Wrong dataset URI: " + datasetUri, datasetUri.toString().endsWith("/thredds/fileServer/fx3/gbr4_v2/gbr4_simple_2010-09.nc"));
                found_2010_09 = true;
            }
        }

        Assert.assertTrue("Dataset gbr4_simple_2019-01.nc is missing", found_2019_01);
        Assert.assertTrue("Dataset gbr4_simple_2016-03.nc is missing", found_2016_03);
        Assert.assertTrue("Dataset gbr4_simple_2010-09.nc is missing", found_2010_09);
    }


    @Test
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
    }


    @Test
    public void testDatasetFileFilter() throws Exception {
        //URL catalogueUrl = new URL("http://dapds00.nci.org.au/thredds/catalog/fx3/gbr1_2.0/catalog.xml");
        URL catalogueUrl = NetCDFDownloadManagerTest.class.getClassLoader().getResource("catalogue/nci_ereefs_gbr1v2_catalog.xml");
        Assert.assertNotNull("The catalogue XML file could not be found in test resources folder.", catalogueUrl);

        String downloadDefinitionStr = JSONUtils.streamToString(
                NetCDFDownloadManagerTest.class.getClassLoader().getResourceAsStream("downloadDefinition/gbr1_exposure.json"), true);
        downloadDefinitionStr = downloadDefinitionStr.replace("${CATALOGUE_URL}", catalogueUrl.toString());

        DownloadBean downloadBean = new DownloadBean(new JSONObject(downloadDefinitionStr));

        LOGGER.debug(downloadBean.toString());

        NetCDFDownloadManager downloadManager = new NetCDFDownloadManager(downloadBean);

        List<NetCDFDownloadManager.CatalogueEntry> catalogueEntries = downloadManager.getCatalogues();
        Assert.assertNotNull("List of catalogue is null", catalogueEntries);
        Assert.assertEquals("List of catalogue contains wrong number of catalogue", 1, catalogueEntries.size());

        // Get all datasets, recursively
        Map<String, NetCDFDownloadManager.DatasetEntry> datasetEntryMap = downloadManager.getDatasets();
        Assert.assertNotNull("Dataset map is null", datasetEntryMap);

        // Ensure all datasets are for exposure
        for (NetCDFDownloadManager.DatasetEntry datasetEntry : datasetEntryMap.values()) {
            Dataset dataset = datasetEntry.getDataset();
            String filename = FilenameUtils.getName(dataset.getUrlPath());

            Assert.assertTrue("Unexpected dataset: " + filename + ". The filename doesn't match the filenameTemplate.",
                    filename.startsWith("gbr1_exposure_maps_"));
        }
    }
}
