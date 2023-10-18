package dal;

import entity.CollectorDatalakeMapping;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MetaData implements Serializable {


    private static final long serialVersionUID = 4019204746109494238L;

    private static final Logger log = LoggerFactory.getLogger(MetaData.class);

    //all fields must be initialized, because of Jackson seriallizer
    private CollectorDatalakeMapping collectorDatalakeMapping = new CollectorDatalakeMapping();

    /*
    private List<TargetTable> eventTables = new ArrayList<>(1);

    private List<GdprIncompliantField> incompliantFields = new ArrayList<>(1);

    private List<GdprCleanValue> cleanValues = new ArrayList<>(1);


     */

    /**
     * Must have default constructor in order to be serializable
     */
    public MetaData() {
    }

    /*
    public MetaData(CollectorDatalakeMapping collectorDatalakeMapping, List<TargetTable> eventTables,
                    List<GdprIncompliantField> incompliantFields, List<GdprCleanValue> cleanValues) {
        Validate.notNull(collectorDatalakeMapping);
        this.collectorDatalakeMapping = collectorDatalakeMapping;

        Validate.notNull(eventTables);
        this.eventTables = eventTables;

        this.incompliantFields = incompliantFields;

        this.cleanValues = cleanValues;
    }


     */

    /**
     * Return the datalake associated with the schema
     *
     * @return datalake for the collector
     */
    public String getDatalake() {
        return collectorDatalakeMapping.getDatalakeName();
    }
}
