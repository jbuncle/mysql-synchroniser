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
import com.jbuncle.mysqlsynchroniser.structure.objects.Column;
import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class TableDiff {

    public static List<String> diff(final Table source, final Table target) {
        try {
            final List<String> updates = new LinkedList<>();
            //Get deleted columns
            updates.addAll(getDeletedColumns(source, target));

            //Get updated columns
            updates.addAll(getUpdatedColumns(source, target));

            //Get deleted indexes
            updates.addAll(getDeletedIndexes(source, target));

            //Get updated indexes
            updates.addAll(getUpdatedIndexes(source, target));

            return updates;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static List<String> getUpdatedColumns(final Table source, final Table target) throws SQLException {
        final List<String> updates = new LinkedList<>();
        Column lastColumn = null;
        for (final Column column : source.getColumns()) {
            ColumnStatementBuilder scriptBuilder = new ColumnStatementBuilder(target.getTableName(), column);
            final Column targetColumn = target.getColumn(column.getName());
            if (targetColumn == null) {
                //Missing column, add it
                if (lastColumn == null) {
                    //First column
                    updates.add(scriptBuilder.getInsertColumnFirstStatement());
                } else {
                    //Add after last column
                    updates.add(scriptBuilder.getInsertColumnAfterStatement(lastColumn));
                }
            } else if (!column.equals(targetColumn)) {
                updates.add(scriptBuilder.getUpdateStatement());
            }
            lastColumn = column;
        }
        return updates;
    }

    private static List<String> getUpdatedIndexes(final Table source, final Table target) throws SQLException {
        final List<String> updates = new LinkedList<>();
        for (final Index sourceIndex : source.getIndexes()) {
            final String sourceKeyName = sourceIndex.getKeyName();
            final Index targetIndex = target.getIndex(sourceKeyName);
            if (targetIndex == null) {
                //Get index add
                updates.add(sourceIndex.getCreateStatement());
            } else if (!sourceIndex.equals(targetIndex)) {
                //Different, drop then add
                updates.add(sourceIndex.getDeleteStatament());
                updates.add(sourceIndex.getCreateStatement());
            }
        }
        return updates;
    }

    private static List<String> getDeletedIndexes(final Table source, final Table target) throws SQLException {
        final List<String> updates = new LinkedList<>();

        for (final Index index : target.getIndexes()) {
            final String indexKeyName = index.getKeyName();
            final Index sourceIndex = source.getIndex(indexKeyName);
            if (sourceIndex == null) {
                //Get delete index
                updates.add(index.getDeleteStatament());
            }
        }
        return updates;
    }

    private static List<String> getDeletedColumns(final Table source, final Table target) throws SQLException {
        final List<String> updates = new LinkedList<>();
        //Work out deletes
        for (final Column column : target.getColumns()) {
            final Column thisColumn = source.getColumn(column.getName());
            final ColumnStatementBuilder scriptBuilder = new ColumnStatementBuilder(target.getTableName(), column);
            if (thisColumn == null) {
                //Extra column in target
                updates.add(scriptBuilder.getDeleteStatement());
            }
        }
        return updates;
    }

}
