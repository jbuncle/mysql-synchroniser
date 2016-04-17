package com.jbuncle.mysqlsynchroniser;

import com.jbuncle.mysqlsynchroniser.structure.MySQL;
import static com.jbuncle.mysqlsynchroniser.structure.MySQL.loadTable;
import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.structure.objects.Database;
import com.jbuncle.mysqlsynchroniser.structure.diff.DatabaseDiff;
import com.jbuncle.mysqlsynchroniser.structure.diff.TableDiff;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author James Buncle
 */
public class ScriptGenerator {

    public static List<String> compareTable(
            final Connection source,
            final Connection target,
            final String table)
            throws SQLException {

        final Table sourceTable = loadTable(source, table);
        final Table targetTable = loadTable(target, table);

        return TableDiff.diff(sourceTable, targetTable);
    }

    /**
     * Generates a List of MySQL Statements to update/synchronise the given target database structure based on the
     * source database.
     *
     * @param sourceConnection the database connection used as the source
     * @param targetConnection the target database connection to create update statements for
     * @return a list MySQL statements created by comparing the source schema to the target schema
     * @throws SQLException
     */
    public static List<String> compareSchema(
            final Connection sourceConnection,
            final Connection targetConnection)
            throws SQLException {

        final Database sourceDatabase = new MySQL(sourceConnection).loadDatabase();
        final Database targetDatabase = new MySQL(targetConnection).loadDatabase();

        final DatabaseDiff diff = new DatabaseDiff(sourceDatabase, targetDatabase);
        return diff.diff(sourceConnection);
    }

}
