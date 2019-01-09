package org.apache.cassandra.tools.cosmosdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.tools.cosmosdb.CosmosCommitLogReadHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommitLogCosmosReplayer 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogCosmosReplayer.class);
	/**
    private final WatchService watcher;
    private final Path dir;
    private final WatchKey key;
    private final CommitLogReader commitLogReader;
    private final CosmosCommitLogReadHandler commitLogReadHander;

     * Creates a WatchService and registers the given directory
    public CommitLogCosmosReplayer(Map<String, Object> configuration) throws IOException {
        this.dir = Paths.get((String) YamlUtils.select(configuration, "cassandra.cdc_raw_directory"));
        watcher = FileSystems.getDefault().newWatchService();
        key = dir.register(watcher, ENTRY_CREATE);
        commitLogReader = new CommitLogReader();
        commitLogReadHander = new CosmosCommitLogReadHandler(keyspace, table);
        DatabaseDescriptor.toolInitialization();
        Schema.instance.loadFromDisk(false);
    }
     */

    /**
     * Process all events for keys queued to the watcher
     *
     * @throws InterruptedException
     * @throws IOException
    public void processEvents() throws InterruptedException, IOException {
        while (true) {
            WatchKey aKey = watcher.take();
            if (!key.equals(aKey)) {
                LOGGER.error("WatchKey not recognized.");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind != ENTRY_CREATE) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path relativePath = ev.context();
                Path absolutePath = dir.resolve(relativePath);
                processCommitLogSegment(absolutePath);
                Files.delete(absolutePath);
                // print out event
                LOGGER.debug("{}: {}", event.kind().name(), absolutePath);
            }
            key.reset();
        }
    }

    public static void xmain(String[] args) throws IOException, InterruptedException {
        Map<String, Object> configuration = YamlUtils.load(args[0]);
        new CommitLogCosmosReplayer(configuration).processEvents();
    }

    private void processCommitLogSegment(Path path) throws IOException {
        LOGGER.debug("Processing commitlog segment...");
        commitLogReader.readCommitLogSegment(commitLogReadHander, path.toFile(), false);
        LOGGER.debug("Commitlog segment processed.");
    }
     */
	
    /**
     * Given arguments specifying a commit log, export the contents of the commit log to CosmosDb's cassandra API.
     *
     * @param args
     *            command lines arguments
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException
    {
    	CommitLogReader reader = new CommitLogReader();
    	String filename = args[0];
    	String keyspace = args[1];
    	String table = args[2];
    	DatabaseDescriptor.loadConfig();
    	CosmosCommitLogReadHandler handler = new CosmosCommitLogReadHandler(keyspace, table);
    	File commitlogFile = new File(filename);
    	reader.readAllFiles(handler, new File[] {commitlogFile});
    	//reader.readCommitLogSegment(handler, comitlogFile, false);
    }
}
