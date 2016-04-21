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
package com.jbuncle.mysqlsynchroniser.structure.diff;

import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.structure.objects.Database;
import com.jbuncle.mysqlsynchroniser.structure.objects.View;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class DatabaseDiff {

    private final Database source;
    private final Database target;

    public DatabaseDiff(final Database source, final Database target) {
        this.source = source;
        this.target = target;
    }

    private Database getSource() {
        return source;
    }

    private Database getTarget() {
        return target;
    }

    private List<String> getTableUpdates(final DataSource sourceConnection) throws SQLException {

        final List<String> updates = new LinkedList<>();
        final Map<String, Table> targetTables = this.getTarget().getTables();
        final Map<String, Table> tables = this.getSource().getTables();

        //Iterate source tables
        for (final Map.Entry<String, Table> sourceEntry : tables.entrySet()) {
            if (targetTables.containsKey(sourceEntry.getKey())) {
                //Has table, compare and update if necessary
                updates.addAll(TableDiff.diff(sourceEntry.getValue(), targetTables.get(sourceEntry.getKey())));
            } else {
                TableStatementBuilder tableStatementBuilder = new TableStatementBuilder(tables.get(sourceEntry.getKey()));
                //Target is missing table, get updates
                updates.add(tableStatementBuilder.getCreateStatement(sourceConnection));
            }
            targetTables.remove(sourceEntry.getKey());
        }
        //Iterate remaining target tables for deletes
        for (final Table targetTable : targetTables.values()) {
            TableStatementBuilder tableStatementBuilder = new TableStatementBuilder(targetTable);
            updates.add(tableStatementBuilder.getDropStatement());
        }
        return updates;
    }

    private List<String> getViewUpdates() throws SQLException {

        final List<String> updates = new LinkedList<>();
        final Map<String, View> targetViews = this.getTarget().getViews();
        final Map<String, View> sourceViews = this.getSource().getViews();

        //Iterate source tables
        for (final Map.Entry<String, View> sourceEntry : sourceViews.entrySet()) {
            if (targetViews.containsKey(sourceEntry.getKey())) {
                ViewDiff diff = new ViewDiff(sourceEntry.getValue(), targetViews.get(sourceEntry.getKey()));
                //Has table, compare and update if necessary
                updates.addAll(diff.diff());
            } else {
                //Target is missing table, get updates
                updates.add(sourceEntry.getValue().getCreateStatement());
            }
            targetViews.remove(sourceEntry.getKey());
        }
        //Iterate remaining target tables for deletes
        for (final View targetTable : targetViews.values()) {
            updates.add(targetTable.getDropStatement());
        }
        return updates;
    }

    public List<String> diff(final DataSource sourceConnection) throws SQLException {
        final List<String> list = new LinkedList<>();
        list.addAll(getTableUpdates(sourceConnection));
        list.addAll(getViewUpdates());
        return list;
    }
}
