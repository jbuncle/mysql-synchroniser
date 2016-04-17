/*
 * The MIT License
 *
 * Copyright 2016 James Buncle <jbuncle@hotmail.com>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.jbuncle.mysqlsynchroniser;

import com.jbuncle.mysqlsynchroniser.util.ConnectionStrategy;
import static com.jbuncle.mysqlsynchroniser.util.ListUtils.implode;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import junit.framework.TestCase;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class ScriptGeneratorTest extends TestCase {

    private ConnectionStrategy source;
    private ConnectionStrategy target;

    public ScriptGeneratorTest(final String testName) {
        super(testName);
    }

    private static MysqlDataSource createDataSource(final String host, final String user, final String password) {
        final MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setServerName(host);
        return dataSource;
    }

    private static MysqlDataSource createDataSource(final String host, final String schema, final String user, final String password) {
        final MysqlDataSource dataSource = createDataSource(host, user, password);
        dataSource.setDatabaseName(schema);
        return dataSource;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        MysqlDataSource dataSource = createDataSource("192.168.99.101", "root", "");
        new ConnectionStrategy(dataSource).update(
                "DROP DATABASE IF EXISTS source;",
                "DROP DATABASE IF EXISTS target;",
                "CREATE DATABASE source;",
                "CREATE DATABASE target;"
        );

        //192.168.99.101:3306
        this.source = new ConnectionStrategy(createDataSource("192.168.99.101", "source", "root", ""));
        this.target = new ConnectionStrategy(createDataSource("192.168.99.101", "target", "root", ""));

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        source.update("DROP DATABASE source;");
        target.update("DROP DATABASE target;");
    }

    /**
     * Test of compareTable method, of class ScriptGenerator.
     */
    public void testCompareTable() throws Exception {
        System.out.println("compareTable");
        source.update("CREATE TABLE pet ("
                + "name VARCHAR(20), "
                + "owner VARCHAR(20), "
                + "species VARCHAR(20), "
                + "sex CHAR(1), "
                + "birth DATE, "
                + "death DATE NOT NULL,"
                + "CONSTRAINT pk_PersonID PRIMARY KEY (name),"
                + "UNIQUE KEY `mykey` (`owner`, `species`)"
                + ");");
        target.update("CREATE TABLE pet ("
                + "name VARCHAR(20), "
                + "owner VARCHAR(20), "
                + "species VARCHAR(20) NOT NULL, "
                + "type VARCHAR(20) NOT NULL, "
                + "death DATE, "
                + "UNIQUE KEY `somekey` (`death`)"
                + ");");

        final String table = "pet";
        final String expResult
                = "ALTER TABLE `pet` DROP `type`;"
                + "ALTER TABLE `pet` CHANGE `name` `name` varchar(20) COLLATE latin1_swedish_ci NOT NULL  COMMENT '';"
                + "ALTER TABLE `pet` CHANGE `owner` `owner` varchar(20) COLLATE latin1_swedish_ci NULL  COMMENT '';"
                + "ALTER TABLE `pet` CHANGE `species` `species` varchar(20) COLLATE latin1_swedish_ci NULL  COMMENT '';"
                + "ALTER TABLE `pet` ADD `sex` char(1) COLLATE latin1_swedish_ci NULL  COMMENT '' AFTER `species`;"
                + "ALTER TABLE `pet` ADD `birth` date NULL  COMMENT '' AFTER `sex`;"
                + "ALTER TABLE `pet` CHANGE `death` `death` date NOT NULL  COMMENT '';"
                + "DROP INDEX `somekey` ON `pet`;"
                + "ALTER TABLE `pet` ADD PRIMARY KEY(`name`);"
                + "ALTER TABLE `pet` ADD UNIQUE `mykey` (`owner`, `species`);";
        final List<String> result = ScriptGenerator.compareTable(source.getDataSource(), target.getDataSource(), table);

        assertEquals(expResult, implode("", result));

        target.update(result);

        compareQueries(source.getConnection(), target.getConnection(), "DESCRIBE " + table);
        compareQueries(source.getConnection(), target.getConnection(), "SHOW INDEXES FROM " + table);
    }

    private static void compareQueries(final Connection source, final Connection target, final String query)
            throws SQLException {
        final ResultSet sourceRs = source.createStatement().executeQuery(query);
        final ResultSet targetRs = target.createStatement().executeQuery(query);
        while (sourceRs.next() && targetRs.next()) {
            final int sourceColCount = sourceRs.getMetaData().getColumnCount();
            final int targetColCount = targetRs.getMetaData().getColumnCount();

            if (sourceColCount != targetColCount) {
                fail("Column count differs in length");
            }
            for (int i = 1; i <= sourceColCount; i++) {
                assertEquals(sourceRs.getObject(i), targetRs.getObject(i));
            }
        }
        if (sourceRs.next() ^ targetRs.next()) {
            fail("Result sets differ in length");
        }
    }
}
