package org.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.sql.DataSource;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;

import com.mongodb.MongoClient;

public class BasicStart {
    static Repository repository;
    static Session session;
    static FileStore fs;
    static DocumentNodeStore ns;

    public static void main(String[] args) {
        int key;
        System.out.println("Enter the type of repository(1-4) :");
        System.out.println("1: Memory.");
        System.out.println("2: Segment TAR.");
        System.out.println("3: Mongo.");
        System.out.println("4: RDB.");
        System.out.println("5: Azure Blob Store.");
        //key = new Scanner(System.in).nextInt();
        key = 5;
        System.out.println("Enter the file system directory to upload to repository: ");
        //var fsPath = new Scanner(System.in).nextLine();
        var fsPath = "/home/grupoitss/dev-tools";
        switch (key) {
            case 1:
                getMemoryNSRepository();
                try {
                    //sessionSave();
                    session = repository.login(getAdminCredentials());
                    FSToRepositoryStore.createNodes(session, fsPath, 10);
                    //NodePrinter.print(session.getRootNode());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } finally {
                    session.logout();
                }
                break;
            case 2:
                getSegmentNSRepository();
                try {
                    //sessionSave();
                    session = repository.login(getAdminCredentials());
                    FSToRepositoryStore.createNodes(session, fsPath, 10);
                    //NodePrinter.print(session.getRootNode());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } finally {
                    session.logout();
                    fs.close();
                }
                break;
            case 3:
                getDocumentMongoNSRepository();
                try {
                    //sessionSave();
                    session = repository.login(getAdminCredentials());
                    FSToRepositoryStore.createNodes(session, fsPath, 10);
                    //NodePrinter.print(session.getRootNode());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } finally {
                    session.logout();
                    ns.dispose();
                }
                break;
            case 4:
                getDocumentRDBNSRepository();
                try {
                    //sessionSave();
                    session = repository.login(getAdminCredentials());
                    FSToRepositoryStore.createNodes(session, fsPath, 10);
                    //NodePrinter.print(session.getRootNode());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } finally {
                    session.logout();
                    ns.dispose();
                }
                break;
            case 5:
                getDocumentAzureDataStoreWitRDBNSRepository();
                try {
                    //sessionSave();
                    session = repository.login(getAdminCredentials());
                    FSToRepositoryStore.createNodes(session, fsPath, 10);
                    //NodePrinter.print(session.getRootNode());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } finally {
                    session.logout();
                    ns.dispose();
                }
                break;
            default:
                System.out.println("Wrong choice!");
                break;
        }
    }

    public static final Repository getSegmentNSRepository() {
        if (repository == null) {
            try {
                fs = FileStoreBuilder.fileStoreBuilder(new File("TARMK-repository"))
                        .withBlobStore((BlobStore) new FileBlobStore("TARMK-repository/blob")).build();
                SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
                repository = new Jcr(new Oak(ns)).createRepository();
            } catch (InvalidFileStoreVersionException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return repository;
    }

    public static final Repository getMemoryNSRepository() {
        if (repository == null) {
            repository = new Jcr(new Oak(new MemoryNodeStore())).createRepository();// Repository repo = new Jcr(new
            // Oak()).createRepository();
        }
        return repository;
    }

    public static final Repository getDocumentMongoNSRepository() {
        @SuppressWarnings({"deprecation", "resource"})
        var mongoClient = new MongoClient("127.0.0.1", 27017);
        ns = MongoDocumentNodeStoreBuilder
                .newMongoDocumentNodeStoreBuilder()
                .setBlobStore((BlobStore) new FileBlobStore("MongoMK-repository/blob"))
                .setMongoDB(mongoClient, "MongoMK")
                .build();
        return repository = new Jcr(new Oak(ns)).createRepository();
    }

    public static final Repository getDocumentRDBNSRepository() {
        DataSource ds = RDBDataSourceFactory.forJdbcUrl("jdbc:postgresql://localhost:5432/rdbmkoak", "postgres", "postgres");
        RDBOptions options = new RDBOptions().tablePrefix("RDBMK_").dropTablesOnClose(false);
        ns = RDBDocumentNodeStoreBuilder
                .newRDBDocumentNodeStoreBuilder()
                .setBlobStore((BlobStore) new FileBlobStore("RDBMK-repository/blob"))
                .setRDBConnection(ds, options)
                .build();
        return repository = new Jcr(new Oak(ns)).createRepository();
    }

    public static final Repository getDocumentAzureDataStoreWitRDBNSRepository() {
        DataSource ds = RDBDataSourceFactory.forJdbcUrl("jdbc:postgresql://localhost:5432/rdbmkoak", "postgres", "postgres");
        RDBOptions options = new RDBOptions().tablePrefix("RDBMK_").dropTablesOnClose(false);
        try {
            ns = RDBDocumentNodeStoreBuilder
                    .newRDBDocumentNodeStoreBuilder()
                    .setBlobStore(createAzureBlobStore())
                    .setRDBConnection(ds, options)
                    .build();
        } catch (DataStoreException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return repository = new Jcr(new Oak(ns)).createRepository();
    }

    private static DataStoreBlobStore createAzureBlobStore() throws DataStoreException {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_CONTAINER_NAME, "devstoreaccount1");
        properties.put(AzureConstants.AZURE_CREATE_CONTAINER, true);
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "http://127.0.0.1:10000/devstoreaccount1");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "devstoreaccount1");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "UseDevelopmentStorage=true");

        String connectionString = Utils.getConnectionStringFromProperties(properties);
        System.out.println(connectionString);
        // Set up an Azure BlobStore
        AzureDataStore azureDataStore = new AzureDataStore();
        azureDataStore.setProperties(properties);
        azureDataStore.init("/local-repository");
        DataStoreBlobStore dsbs = new DataStoreBlobStore(azureDataStore);
        return dsbs;
    }

    public static void sessionSave() throws RepositoryException {
        session = repository.login(getAdminCredentials());
        Node root = session.getRootNode();
        if (root.hasNode("hello")) {
            Node hello = root.getNode("hello");
            long count = hello.getProperty("count").getLong();
            hello.setProperty("count", count + 1);
            try {
                hello.setProperty("file", new FileInputStream("/tmp/LargeFile1.bin"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            System.out.println("found the hello node, count = " + count);
        } else {
            System.out.println("creating the hello node");
            root.addNode("hello").setProperty("count", 1);
            Node hello = root.getNode("hello");
            try {
                hello.setProperty("file", new FileInputStream("/tmp/LargeFile2.bin"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        session.save();
    }

    protected static SimpleCredentials getAdminCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

}