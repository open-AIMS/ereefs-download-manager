/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.Utils;
import au.gov.aims.ereefs.database.DatabaseClient;
import au.gov.aims.ereefs.database.table.DatabaseTable;
import au.gov.aims.ereefs.helper.NcAnimateConfigHelper;
import au.gov.aims.ereefs.helper.TestHelper;
import com.mongodb.ServerAddress;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.net.InetSocketAddress;

public class DatabaseTestBase {
    private static final String DATABASE_NAME = "testdb";

    private MongoServer server;
    private DatabaseClient databaseClient;

    public DatabaseClient getDatabaseClient() {
        return this.databaseClient;
    }

    @Before
    public void init() throws Exception {
        File dbCacheDir = DatabaseTable.getDatabaseCacheDirectory();
        Utils.deleteDirectory(dbCacheDir);

        this.server = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = this.server.bind();

        this.databaseClient = new DatabaseClient(new ServerAddress(serverAddress), DATABASE_NAME);
        TestHelper.createTables(this.databaseClient);
    }

    @After
    public void shutdown() {
        NcAnimateConfigHelper.clearMetadataCache();
        if (this.server != null) {
            this.server.shutdown();
        }
    }
}
