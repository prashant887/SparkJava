package util;

import config.SparkJobSettings;
import etl.Constants;
import hadoop.HadoopConfigurationAdapter;
import hcat.HCatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ParquetStagingUtil {

    private static final Logger log = LoggerFactory.getLogger(ParquetStagingUtil.class);
    private static long RETENTION_PERIOD_MS = TimeUnit.DAYS.toMillis(1);
    private static final String SEP = "__";
    /** Parquet file extension */
    private static final String PARQUET_EXTENSION = ".parq";
    public static String OFFICIAL_PARQUET_DIR = "OFFICIAL_PARQUET_TO_LOAD";
    static final String SCRATCH_PREFIX= "SCRATCH_";
    static final String MOVE_IN_PROGRESS = "MOVE_IN_PROGRESS";
    static final String SCHEMA_CHANGE_DETECTED = "SCHEMA_CHANGE_DETECTED";
    static final String IGNORE = "IGNORE_THESE_FILES";
    static final String LOAD_IN_PROGRESS = "LOAD_IN_PROGRESS";
    private static final long DEFAULT_SCHEMA_VERSION = 1;

    /**
     * Cleans the staging directory on HDFS. Since it is possible to have multiple jobs for a single collector, only files older
     * than a day will be deleted. This is to prevent deletion of valid files that are in the process of being loaded.
     *
     * @param jobSettings
     * @throws IOException
     */

    public static void cleanStagingHdfsDir(SparkJobSettings jobSettings) throws IOException {
        Configuration conf = new Configuration();
        HadoopConfigurationAdapter.setupConfiguration(conf, jobSettings.clusterSettings);
        FileSystem hdfs = FileSystem.get(conf);
        Path rootDir = new Path(jobSettings.stagingHdfsDir + Path.SEPARATOR + jobSettings.collectorIds);

        tryToDelete(hdfs, rootDir, jobSettings.clusterSettings.getHistoryDatabase());
    }

    static void tryToDelete(FileSystem hdfs, Path rootDir, String historyDb) throws IOException {
        Path hdfsScratchDir = new Path(rootDir + Path.SEPARATOR + SCRATCH_PREFIX + historyDb + Path.SEPARATOR);
        log.info("Deleting the following staging HDFS directory: {}.", hdfsScratchDir);
        if (hdfs.exists(hdfsScratchDir)) {
            try {
                hdfs.delete(hdfsScratchDir, true);
            } catch (IOException e) {
                log.error("Exception caught while deleting staging directory.", e);
            }
        }
        Path hdfsOfficialParquetLoadDir = new Path(rootDir + Path.SEPARATOR + OFFICIAL_PARQUET_DIR + Path.SEPARATOR);
        log.info("Clearing the following HDFS directory: {} . Will delete empty directories and directories older than 1 day.", hdfsOfficialParquetLoadDir);
        if (hdfs.exists(hdfsOfficialParquetLoadDir)) {
            try {
                deleteWithRetention(hdfs, hdfsOfficialParquetLoadDir);
            } catch (IOException e) {
                log.error("Exception caught while clearing staging directory.", e);
            }
        }
    }

    private static void deleteWithRetention(FileSystem hdfs, Path hdfsPath) throws FileNotFoundException, IOException {
        long currentTimeMs = System.currentTimeMillis();
        FileStatus[] status = hdfs.listStatus(hdfsPath);
        if (status.length == 0) { // current directory is empty, delete it
            hdfs.delete(hdfsPath, false);
            return;
        }

        for (FileStatus fs : status) {
            if (currentTimeMs - fs.getModificationTime() >= RETENTION_PERIOD_MS) {
                hdfs.delete(fs.getPath(), true);
            }
        }

        // Check if the current directory is empty and if it is, delete it
        status = hdfs.listStatus(hdfsPath);
        if (status.length == 0) {
            hdfs.delete(hdfsPath, false);
        }
    }
    /**
     * Generates an HDFS path for a parquet file.
     *
     * @param jobSettings
     *           Used to get the HDFS staging directory
     * @param table
     *           Used as a directory in the generated path
     * @param arrivalDay
     *           Used as a directory in the generated path
     * @param collectorId
     *           Used as a directory in the generated path
     * @param kafkaInfo
     *           Used as part of the parquet filename
     *           Contains the kafka partition and offset information to identify the file
     * @return The HDFS path for the parquet file
     */
    public static String genParquetFilePath(SparkJobSettings jobSettings, String table, long arrivalDay, String collectorId,
                                            String kafkaInfo) {
        String dir = jobSettings.stagingHdfsDir + Path.SEPARATOR + collectorId + Path.SEPARATOR + SCRATCH_PREFIX + jobSettings.clusterSettings.getHistoryDatabase() + Path.SEPARATOR;
        String file = table + SEP + arrivalDay + SEP + kafkaInfo.replace(" ", "_") + SEP + UUID.randomUUID() + PARQUET_EXTENSION;
        return dir + file;
    }

    /**
     * Generate the name of the directory a parquet file will be loaded from
     */
    private static Path getTempPath(ParquetInfo pi, String parentStagingDir, String uid) {
        // get the expected directory
        String collector = pi.getCollectorId();
        String tableName = pi.getTable();
        String arrivalPeriod = Long.toString(pi.getArrivalPeriod());
        Path stagingPath = getStagingPath(parentStagingDir, collector, uid);
        Path loadPath = new Path(stagingPath, tableName + Path.SEPARATOR + arrivalPeriod);
        return loadPath;
    }
    private static Path getStagingPath(String parentStagingDir, String collector, String uid) {
        Path parentStaging = new Path(parentStagingDir);
        return new Path(parentStaging, collector + Path.SEPARATOR + uid + Path.SEPARATOR);
    }

    public static void moveFilesAndSetLoadPaths(List<ParquetInfo> parquetInfos, FileSystem hdfs, String parentStagingDir, String uid) throws IOException {
        try {
            assert parquetInfos.size() > 0;
            String collector = parquetInfos.get(0).getCollectorId();
            Path inProgress = getMoveInProgressIndicator(parentStagingDir, collector, uid);
            addInProgressIndicator(hdfs, inProgress);
            for (ParquetInfo pi : parquetInfos) {
                Path srcFile = new Path(pi.getFilePath());
                Path destPath = getTempPath(pi, parentStagingDir, uid);
                if (!hdfs.exists(destPath)) {
                    hdfs.mkdirs(destPath);
                }
                hdfs.rename(srcFile, new Path(destPath, srcFile.getName()));
                pi.setLoadDirPath(destPath);
            }
            removeInProgressIndicator(hdfs, inProgress);
        } catch (IOException e) {
            log.error("Error moving parquet file to loading directory", e);
            throw e;
        }
    }
    /**
     * Returns a staging path for official streaming jobs.
     */
    public static String getOfficialTempPath(String uid) {
        return OFFICIAL_PARQUET_DIR + Path.SEPARATOR + uid;
    }

    /**
     * Adds an indicator that the schema has changed for the table.
     */
    public static void markSchemaChanged(FileSystem hdfs, String parentStagingDir, String collector, String uid, String table) {
        Path stagingPath = getStagingPath(parentStagingDir, collector, uid);
        Path schemaChanged = new Path(stagingPath, table + Path.SEPARATOR + SCHEMA_CHANGE_DETECTED);
        try {
            log.info("Adding file to mark schema as changed: {}", schemaChanged);
            hdfs.createNewFile(schemaChanged);
        } catch (IOException e) {
            log.error("Failed to mark that {} schema has changed", table, e);
        }
    }
    /**
     * Adds an indicator that a load is in-progress.
     */
    public static void markLoadInProgress(FileSystem hdfs, String parentStagingDir, String collector, String uid) {
        Path loadInProgress = getLoadInProgressIndicator(parentStagingDir, collector, uid);
        try {
            hdfs.createNewFile(loadInProgress);
        } catch (IOException e) {
            log.error("Failed to mark that load is in-progress", e);
        }
    }

    /**
     * Removes the load is in-progress indicator.
     */
    public static void unmarkLoadInProgress(FileSystem hdfs, String parentStagingDir, String collector, String uid) {
        Path loadInProgress = getLoadInProgressIndicator(parentStagingDir, collector, uid);
        try {
            if (hdfs.exists(loadInProgress)) {
                hdfs.delete(loadInProgress, false);
            } else {
                log.warn("Load in-progress indicator not found.");
            }
        } catch (IOException e) {
            log.error("Failed to remove load in-progress indicator", e);
        }
    }
    private static Path getLoadInProgressIndicator(String parentStagingDir, String collector, String uid) {
        Path stagingPath = getStagingPath(parentStagingDir, collector, uid);
        return new Path(stagingPath, LOAD_IN_PROGRESS);
    }

    private static Path getMoveInProgressIndicator(String parentStagingDir, String collector, String uid) {
        Path stagingPath = getStagingPath(parentStagingDir, collector, uid);
        return new Path(stagingPath, MOVE_IN_PROGRESS);
    }

    private static void addInProgressIndicator(FileSystem hdfs, Path inProg) throws IOException {
        if (hdfs.exists(inProg)) {
            // If HIVE loads are multi-threaded, then HIVE will throw an exception if it tries to load data
            // from an empty directory.
            log.warn("Another set of parquet files are being moved.");
        } else {
            hdfs.createNewFile(inProg);
        }
    }
    private static void removeInProgressIndicator(FileSystem hdfs, Path inProg) throws IOException {
        if (hdfs.exists(inProg)) {
            hdfs.delete(inProg, false);
        } else {
            log.warn("In progress indicator not found. Was another set of parquet files moved too?");
        }
    }

    /**
     * Get a list of parquet files from the previous official run.
     */
    public static List<ParquetInfo> getOrphanedParquetFiles(SparkJobSettings jobSettings) throws IOException {
        Path rootDir =
                new Path(jobSettings.stagingHdfsDir + Path.SEPARATOR + jobSettings.collectorIds + Path.SEPARATOR
                        + OFFICIAL_PARQUET_DIR);
        Configuration conf = new Configuration();
        HadoopConfigurationAdapter.setupConfiguration(conf, jobSettings.clusterSettings);
        FileSystem hdfs = FileSystem.get(conf);
        return getOrphanedParquetFiles(hdfs, rootDir, jobSettings.collectorIds);
    }
    static List<ParquetInfo> getOrphanedParquetFiles(FileSystem hdfs, Path rootDir, String collectorId) throws IOException {
        List<Path> parquetFiles = new ArrayList<>();
        if (hdfs.exists(rootDir)) {
            // Go through parquet files from previous jobs
            FileStatus[] status = hdfs.listStatus(rootDir);
            for (FileStatus fs : status) {
                if (fs.isDirectory()) {
                    try {
                        // Only try to load where a load in-progress indicator exists
                        Path loadInProgressIndicator = new Path(fs.getPath(), LOAD_IN_PROGRESS);
                        if (!hdfs.exists(loadInProgressIndicator)) {
                            log.info("Skipping {} since load in-progress indicator is not found", fs.getPath());
                            continue;
                        }

                        parquetFiles.addAll(getOfficialParquetFiles(hdfs, fs.getPath()));
                    } catch (IllegalStateException | UnsupportedOperationException e) {
                        log.info("Skipping batch due to the following: {}", e.getMessage());
                    }
                }
            }
            // Verify that there are only files from one UUID
            int uuidCnt = countUniqueUuids(parquetFiles);
            if (uuidCnt > 1) {
                log.error("Only expected valid orphaned parquet files from one official run, not {}", uuidCnt);
                // Exclude these files in future scans
                markFilesAsIgnore(hdfs, parquetFiles);
                return Collections.emptyList();
            }
        }
        List<ParquetInfo> parquetInfos = toParquetInfo(parquetFiles, collectorId);
        if (!parquetInfos.isEmpty() && !containsChunk(parquetInfos)) {
            log.error("Did not find a Chunk parquet file, will ignore these orphaned files");
            // Exclude these files in future scans
            markFilesAsIgnore(hdfs, parquetFiles);
            return Collections.emptyList();
        }
        return parquetInfos;
    }
    private static int countUniqueUuids(List<Path> parquetFiles) {
        Set<String> uuids = new HashSet<>();
        for (Path p : parquetFiles) {
            String uuid = p.getParent().getParent().getParent().getName();
            uuids.add(uuid);
        }
        return uuids.size();
    }

    private static void markFilesAsIgnore(FileSystem hdfs, List<Path> parquetFiles) throws IOException {
        Set<Path> paths = new HashSet<>();
        for (Path p : parquetFiles) {
            paths.add(p.getParent());
        }
        for (Path p : paths) {
            Path ignore = new Path(p, IGNORE);
            hdfs.createNewFile(ignore);
        }
    }

    private static List<ParquetInfo> toParquetInfo(List<Path> parquetFiles, String collectorId) {
        List<ParquetInfo> parquetInfos = new ArrayList<>();
        for (Path p : parquetFiles) {
            parquetInfos.add(toParquetInfo(p, collectorId));
        }
        return parquetInfos;
    }

    private static ParquetInfo toParquetInfo(Path p, String collectorId) {
        // Assumes files have the following directory structure:
        // ../OFFICIAL_PARQUET_TO_LOAD/<UUID>/<table_name>/<arrival_day>/<parquet_file>
        String arrivalDay = p.getParent().getName();
        String table = p.getParent().getParent().getName();
        ParquetInfo pi = new ParquetInfo(new Long(arrivalDay), collectorId, DEFAULT_SCHEMA_VERSION, table, p.toString());
        pi.setLoadDirPath(p);
        return pi;
    }
    private static List<Path> getOfficialParquetFiles(FileSystem hdfs, Path hdfsPath) throws IOException {
        List<Path> parquetFiles = new ArrayList<>();

        FileStatus[] status = hdfs.listStatus(hdfsPath);
        if (status.length == 0) { // current directory is empty, delete it
            hdfs.delete(hdfsPath, false);
        }

        for (FileStatus fs : status) {
            if (fs.isFile() && fs.getPath().getName().equals(MOVE_IN_PROGRESS)) {
                log.info("Marker detected: {}", fs.getPath());
                throw new IllegalStateException("Batch was in the middle of moving");
            }
            if (fs.isFile() && fs.getPath().getName().equals(SCHEMA_CHANGE_DETECTED)) {
                log.info("Marker detected: {}", fs.getPath());
                throw new UnsupportedOperationException("Schema change detected in previous run for the following table: "
                        + fs.getPath().getParent().getName());
            }
            if (fs.isFile() && fs.getPath().getName().equals(IGNORE)) {
                log.info("Marker detected: {}", fs.getPath());
                throw new IllegalStateException("Ignore indicator detected");
            }
            if (fs.isFile() && fs.getPath().getName().equals(LOAD_IN_PROGRESS)) {
                continue; // skip this file
            }
            if (fs.isDirectory()) {
                parquetFiles.addAll(getOfficialParquetFiles(hdfs, fs.getPath()));
            } else {
                parquetFiles.add(fs.getPath());
            }
        }
        return parquetFiles;
    }

    /**
     * Checks if the list of parquet files matches the provided schema. If there is a mismatch, then an indicator is added to the
     * table's directory
     */
    public static boolean verifySchemas(List<ParquetInfo> parquetInfos, ParquetSchemas parquetSchemas, SparkJobSettings jobSettings) {
        Configuration conf = new Configuration();
        HadoopConfigurationAdapter.setupConfiguration(conf, jobSettings.clusterSettings);

        for (ParquetInfo pi : parquetInfos) {
            Path path = new Path(pi.getFilePath());
            try {
                ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
                MessageType schema = readFooter.getFileMetaData().getSchema();

                if (!parquetSchemas.matches(pi.getTable(), schema)) {
                    // Do not load any parquet files from this official run
                    log.info("Schema for {} parquet does not match current!", pi.getTable());
                    markTableSchemaChanged(jobSettings, pi);
                    return false;
                }
            } catch (IOException e) {
                log.error("Unable to validate schema for the {} parquet file!", pi.getTable());
                markTableSchemaChanged(jobSettings, pi);
                return false;
            }
        }
        return true;
    }

    private static void markTableSchemaChanged(SparkJobSettings jobSettings, ParquetInfo pi) {
        try {
            FileSystem hdfs = FileSystem.get(HCatalogFactory.getHcatConfiguration(jobSettings.clusterSettings));
            String parentStagingDir = jobSettings.stagingHdfsDir;
            String uuid = pi.getLoadDir().getParent().getParent().getParent().getName();
            markSchemaChanged(hdfs, parentStagingDir, pi.getCollectorId(), getOfficialTempPath(uuid), pi.getTable());
        } catch (IOException e) {
            log.error("Failed to mark {} schema as changed", pi.getTable(), e);
        }
    }

    /*
     * Retrieve Parquet file row/column count information from metadata
     */
    public static void getParquetFileRowColumnCount(List<ParquetInfo> parquetInfos, SparkJobSettings jobSettings) {
        Configuration conf = new Configuration();
        HadoopConfigurationAdapter.setupConfiguration(conf, jobSettings.clusterSettings);
        for (ParquetInfo pi : parquetInfos) {
            Path path = new Path(pi.getFilePath());
            int rowCount = 0;
            try {
                ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
                for(BlockMetaData b : readFooter.getBlocks()){
                    rowCount += b.getRowCount();
                }
                pi.setRowCount(rowCount);
                pi.setColumnCount(readFooter.getFileMetaData().getSchema().getColumns().size());
            } catch (IOException e) {
                log.error("Unable to get row/column count for the {} parquet file!", pi.getTable());
            }
        }
    }

    /**
     * Verifies that the list of parquet files contains at least one for chunk.
     */
    public static boolean containsChunk(List<ParquetInfo> parquetInfos) {
        for (ParquetInfo pi : parquetInfos) {
            if (pi.getTable().equalsIgnoreCase(Constants.CHUNK_TABLE)) {
                return true;
            }
        }
        return false;
    }
}
