package liquibase.diff.output.changelog.core;

import liquibase.change.Change;
import liquibase.change.core.RawSQLChange;
import liquibase.database.Database;
import liquibase.database.core.SybaseDatabase;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.AbstractChangeGenerator;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.MissingObjectChangeGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtils;

public class MissingStoredProcedureChangeGenerator extends AbstractChangeGenerator implements MissingObjectChangeGenerator {
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
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, final Database comparisonDatabase, ChangeGeneratorChain chain) {
    	StoredProcedure storedProcedure = (StoredProcedure) missingObject;

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
