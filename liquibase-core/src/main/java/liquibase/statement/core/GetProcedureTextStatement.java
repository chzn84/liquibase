package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class GetProcedureTextStatement extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String procedureName;

    public GetProcedureTextStatement(String catalogName, String schemaName, String procedureName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.procedureName = procedureName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getProcedureName() {
        return procedureName;
    }
}
