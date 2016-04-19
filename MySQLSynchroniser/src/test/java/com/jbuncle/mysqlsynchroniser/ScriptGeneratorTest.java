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

import com.jbuncle.mysqlsynchroniser.connection.ArrayRowMapper;
import com.jbuncle.mysqlsynchroniser.connection.ConnectionStrategy;
import com.jbuncle.mysqlsynchroniser.connection.RowMapper;
import static com.jbuncle.mysqlsynchroniser.util.ListUtils.implode;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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

        MysqlDataSource dataSource = createDataSource("127.0.0.1", "root", "");
        new ConnectionStrategy(dataSource).update(
                "DROP DATABASE IF EXISTS source;",
                "DROP DATABASE IF EXISTS target;",
                "CREATE DATABASE source;",
                "CREATE DATABASE target;"
        );

        //192.168.99.101:3306
        this.source = new ConnectionStrategy(createDataSource("127.0.0.1", "source", "root", ""));
        this.target = new ConnectionStrategy(createDataSource("127.0.0.1", "target", "root", ""));

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
                + "UNIQUE KEY `mykey` (`owner`, `species`),"
                + "UNIQUE KEY `updatedkey` (`owner`, `death`)"
                + ");");
        target.update("CREATE TABLE pet ("
                + "name VARCHAR(20), "
                + "owner VARCHAR(20), "
                + "species VARCHAR(20) NOT NULL, "
                + "type VARCHAR(20) NOT NULL, "
                + "death DATE, "
                + "UNIQUE KEY `somekey` (`death`),"
                + "UNIQUE KEY `updatedkey` (`owner`)"
                + ");");

        final String table = "pet";
        final List<String> result = ScriptGenerator.compareTable(source.getDataSource(), target.getDataSource(), table);

        target.update(result);

        compareQueries("DESCRIBE " + table, false);
        compareQueries("SHOW INDEXES FROM " + table, true);
    }

    private void compareQueries(final String query, final boolean ignoreOrder)
            throws SQLException {
        final RowMapper<Object[]> rowMapper = new ArrayRowMapper();
        if (ignoreOrder) {
            final LinkedHashSet<Object[]> sourceData = new LinkedHashSet<>(this.source.query(query, rowMapper));
            final LinkedHashSet<Object[]> targetData = new LinkedHashSet<>(this.target.query(query, rowMapper));
            if (sourceData.size() != targetData.size()) {
                fail("Results count differs in length");
            }

            for (Object[] objects : targetData) {
                if (!setContainsArray(objects, sourceData)) {
                    fail("Target data has extra entry: '" + Arrays.toString(objects) + "'");
                }
            }
            for (Object[] objects : sourceData) {
                if (!setContainsArray(objects, targetData)) {
                    fail("Target data missing entry: '" + Arrays.toString(objects) + "'");
                }
            }
        } else {
            final List<Object[]> sourceData = this.source.query(query, rowMapper);
            final List<Object[]> targetData = this.target.query(query, rowMapper);
            for (int i = 0; i < sourceData.size(); i++) {
                if (!Arrays.equals(sourceData.get(i), targetData.get(i))) {
                    fail("Source data "
                            + "'" + Arrays.toString(sourceData.get(i)) + "'"
                            + " is not equal to "
                            + "'" + Arrays.toString(targetData.get(i)) + "'");
                }
            }
        }
    }

    private static boolean setContainsArray(final Object[] arr, final Set<Object[]> set) {
        for (final Object[] objects : set) {
            if (Arrays.equals(objects, arr)) {
                return true;
            }
        }
        return false;
    }

}
