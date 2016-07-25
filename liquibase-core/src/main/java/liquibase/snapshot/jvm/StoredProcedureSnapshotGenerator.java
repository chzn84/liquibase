package liquibase.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.SybaseDatabase;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.statement.core.GetProcedureTextStatement;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtils;

import java.sql.SQLException;
import java.util.List;

public class StoredProcedureSnapshotGenerator extends JdbcSnapshotGenerator {
    public StoredProcedureSnapshotGenerator() {
        super(StoredProcedure.class, new Class[] { Schema.class });
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        if (((StoredProcedure) example).getBody() != null) {
            return example;
        }
        Database database = snapshot.getDatabase();
        Schema schema = example.getSchema();

        List<CachedRow> storedProceduresMetadataRs = null;
        try {
            storedProceduresMetadataRs = ((JdbcDatabaseSnapshot) snapshot).getMetaData().getProcedures(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), example.getName());
            
            if (storedProceduresMetadataRs.size() > 0) {
                CachedRow row = storedProceduresMetadataRs.get(0);
                String rawProcedureName = row.getString("PROCEDURE_NAME");
                String rawSchemaName = StringUtils.trimToNull(row.getString("PROCEDURE_SCHEM"));
                String rawCatalogName = StringUtils.trimToNull(row.getString("PROCEDURE_CAT"));

                StoredProcedure storedProcedure = new StoredProcedure().setName(cleanNameFromDatabase(rawProcedureName, database));
                CatalogAndSchema schemaFromJdbcInfo = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(rawCatalogName, rawSchemaName);
                storedProcedure.setSchema(new Schema(schemaFromJdbcInfo.getCatalogName(), schemaFromJdbcInfo.getSchemaName()));

                if (database instanceof SybaseDatabase) {
                    try {
                        String text = ((SybaseDatabase) database).getProcedureText(schemaFromJdbcInfo, storedProcedure.getName());

                        storedProcedure.setBody(text);
                       /* List<CachedRow> storedProcedureColumnsMetadataRs = ((JdbcDatabaseSnapshot) snapshot).getMetaData()
                            .getProcedureColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema),
                                    ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), rawProcedureName, null);
                    storedProcedure.setColumns(storedProcedureColumnsMetadataRs);*/
                    } catch (DatabaseException e) {
                        throw new DatabaseException("Error getting " + database.getConnection().getURL() + " stored procedure with " + new GetProcedureTextStatement(storedProcedure.getSchema().getCatalogName(), storedProcedure.getSchema().getName(), rawProcedureName), e);
                    }
                }
                return storedProcedure;
            } else  {
                return null;
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot)
            throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(StoredProcedure.class)) {
            return;
        }

        if (foundObject instanceof Schema) {

            Database database = snapshot.getDatabase();
            Schema schema = (Schema) foundObject;

            List<CachedRow> storedProceduresMetadataRs = null;
            try {
                storedProceduresMetadataRs = ((JdbcDatabaseSnapshot) snapshot).getMetaData().getProcedures(
                        ((AbstractJdbcDatabase) database).getJdbcCatalogName(schema),
                        ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), null);
                for (CachedRow row : storedProceduresMetadataRs) {
                    String procedureName = row.getString("PROCEDURE_NAME");
                    String schemaName = StringUtils.trimToNull(row.getString("PROCEDURE_SCHEM"));
                    String catalogName = StringUtils.trimToNull(row.getString("PROCEDURE_CAT"));
                    CatalogAndSchema schemaFromJdbcInfo = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(catalogName, schemaName);
                    StoredProcedure storedProcedure = new StoredProcedure();
                    storedProcedure.setName(cleanNameFromDatabase(procedureName, database)).setSchema(schema);
                    storedProcedure.setSchema(schema);

                    if (database instanceof SybaseDatabase) {
                        try {
                            String text = ((SybaseDatabase) database).getProcedureText(schemaFromJdbcInfo, storedProcedure.getName());
                            storedProcedure.setBody(text);
                        } catch (DatabaseException e) {
                            throw new DatabaseException("Error getting " + database.getConnection().getURL() + " stored procedure with " + new GetProcedureTextStatement(storedProcedure.getSchema().getCatalogName(), storedProcedure.getSchema().getName(), procedureName), e);
                        }
                    }

//                    List<CachedRow> storedProcedureColumnsMetadataRs = ((JdbcDatabaseSnapshot) snapshot).getMetaData()//try these
//                            .getProcedureColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema),//try these
//                                    ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), procedureName, null);//try these
////                    storedProcedure.setColumns(storedProcedureColumnsMetadataRs);//try these
                    schema.addDatabaseObject(storedProcedure);
                }
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }}
