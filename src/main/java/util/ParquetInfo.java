package util;

import org.apache.hadoop.fs.Path;

public class ParquetInfo {
    public static final String POISON_INDICATOR = "__vac__stop__this__job__!";
    public static final String TAINTED_INDICATOR = "__sc__skip__this__batch__!";

    private final long arrivalPeriod;
    private final String collectorId;
    private final long schemaVersion;
    private final String table;
    private final String filePath;
    private Path loadDir;
    private String partitionCol; // TODO: temp for migration
    private String periodType;
    private int rowCount;
    private int columnCount;

    public ParquetInfo(long arrivalPeriod, String collectorId, long schemaVersion, String table, String filePath) {
        this.arrivalPeriod = arrivalPeriod;
        this.collectorId = collectorId;
        this.schemaVersion = schemaVersion;
        this.table = table;
        this.filePath = filePath;
        this.partitionCol = "pa__arrival_day"; // default to partition by day
        this.periodType = "day";
    }

    public ParquetInfo(long arrivalPeriod, String collectorId, long schemaVersion, String table, String filePath,
                       String partitionCol, String periodType) {
        this.arrivalPeriod = arrivalPeriod;
        this.collectorId = collectorId;
        this.schemaVersion = schemaVersion;
        this.table = table;
        this.filePath = filePath;
        this.partitionCol = partitionCol;
        this.periodType = periodType;
    }

    public long getArrivalPeriod() {
        return arrivalPeriod;
    }

    public String getCollectorId() {
        return collectorId;
    }

    public long getSchemaVersion() {
        return schemaVersion;
    }

    public String getTable() {
        return table;
    }

    public String getFilePath() {
        return filePath;
    }

    public static ParquetInfo poisonPill() {
        return new ParquetInfo(0, POISON_INDICATOR, 0, null, null);
    }

    public static Boolean isPoisonPill(ParquetInfo pi) {
        return POISON_INDICATOR.equals(pi.getCollectorId());
    }

    public static ParquetInfo tainted() {
        return new ParquetInfo(0, TAINTED_INDICATOR, 0, null, null);
    }

    public static Boolean isTainted(ParquetInfo pi) {
        return TAINTED_INDICATOR.equals(pi.getCollectorId());
    }

    public Path getLoadDir() {
        return loadDir;
    }

    public void setLoadDirPath(Path loadDir) {
        this.loadDir = loadDir;
    }

    public String getPartitionCol() {
        return partitionCol;
    }

    public String getPeriodType() {
        return periodType;
    }

    public void setToMonthPartition() {
        this.partitionCol = "pa__arrival_period";
        this.periodType = "month";
    }

    public int getRowCount() {
        return this.rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount= rowCount;
    }

    public int getColumnCount() {
        return this.columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount= columnCount;
    }
}
