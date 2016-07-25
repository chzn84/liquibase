package liquibase.sqlgenerator.core;

import static org.junit.Assert.*;
import liquibase.database.core.SybaseDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.GetProcedureTextStatement;
import liquibase.statement.core.GetViewDefinitionStatement;

import org.junit.Test;

public class GetProcedureDefinitionGeneratorSybaseTest {

	@Test
	public void testGenerateSqlForDefaultSchema() {
		GetProcedureTextGeneratorSybase generator = new GetProcedureTextGeneratorSybase();
		GetProcedureTextStatement statement = new GetProcedureTextStatement(null, null, "procedure_name");
		Sql[] sql = generator.generateSql(statement, new SybaseDatabase(), null);
		assertEquals(1, sql.length);
		assertEquals("select text from syscomments where id = object_id('dbo.procedure_name') order by colid", sql[0].toSql());
	}
	
	@Test
	public void testGenerateSqlForNamedSchema() {
		GetProcedureTextGeneratorSybase generator = new GetProcedureTextGeneratorSybase();
		GetProcedureTextStatement statement = new GetProcedureTextStatement(null, "owner", "procedure_name");
		Sql[] sql = generator.generateSql(statement, new SybaseDatabase(), null);
		assertEquals(1, sql.length);
		assertEquals("select text from syscomments where id = object_id('OWNER.procedure_name') order by colid", sql[0].toSql());
	}

}
