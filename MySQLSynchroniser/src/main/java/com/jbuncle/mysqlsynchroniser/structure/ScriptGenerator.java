/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jbuncle.mysqlsynchroniser.structure;

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

        final MySQLBaseTable sourceTable = new MySQLBaseTable(source, table);
        final MySQLBaseTable targetTable = new MySQLBaseTable(source, table);

        return sourceTable.getSynchroniseUpdates(targetTable);
    }

    /**
     * Generates a List of MySQL Statements to update/synchronise the given target
     * database structure based on the source database.
     * 
     * @param source the database connection used as the source
     * @param target the target database connection to create update statements for
     * @return a list MySQL statements created by comparing the source schema to the target schema
     * @throws SQLException 
     */
    public static List<String> compareSchema(final Connection source, final Connection target)
            throws SQLException {

        final MySQLDatabase sourceTable = new MySQLDatabase(source);
        final MySQLDatabase targetTable = new MySQLDatabase(source);

        return sourceTable.getSynchroniseUpdates(targetTable);
    }
}
