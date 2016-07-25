package liquibase.diff.output.changelog.core;

import liquibase.change.Change;
import liquibase.change.core.CreateProcedureChange;
import liquibase.change.core.RawSQLChange;
import liquibase.database.Database;
import liquibase.database.core.SybaseDatabase;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl.SchemaComparison;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.AbstractChangeGenerator;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.ChangedObjectChangeGenerator;
import liquibase.logging.LogFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Index;
import liquibase.structure.core.Schema;
import liquibase.structure.core.StoredProcedure;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import liquibase.util.StringUtils;



public class ChangedStoredProcedureChangeGenerator extends AbstractChangeGenerator implements ChangedObjectChangeGenerator {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {

        if (StoredProcedure.class.isAssignableFrom(objectType)) {
            return PRIORITY_DEFAULT;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Class<? extends DatabaseObject>[] runBeforeTypes() {
    	return null;
    }

    @Override
    public Class<? extends DatabaseObject>[] runAfterTypes() {
        return new Class[] {
                Schema.class
        };
    }

    @Override
    public Change[] fixChanged(DatabaseObject changedObject, ObjectDifferences differences, DiffOutputControl control, Database referenceDatabase, final Database comparisonDatabase, ChangeGeneratorChain chain) {
    	StoredProcedure storedProcedure = (StoredProcedure) changedObject;

        RawSQLChange change = new RawSQLChange();

        String sql = storedProcedure.getBody();
        if ( storedProcedure.getBody() == null) {
            sql = "COULD NOT DETERMINE PROCEDURE BODY";
        } else if (comparisonDatabase instanceof SybaseDatabase) {
            String storedProcedureName;
            if (!control.getIncludeCatalog() && !control.getIncludeSchema()) {
                storedProcedureName = comparisonDatabase.escapeObjectName(storedProcedure.getName(), StoredProcedure.class);
            } else {
                storedProcedureName = comparisonDatabase.escapeViewName(storedProcedure.getSchema().getCatalogName(), storedProcedure.getSchema().getName(), storedProcedure.getName());
            }
             sql = storedProcedure.getBody();
        }
        change.setSql(sql);
        
        return new Change[] { change };
    }
}