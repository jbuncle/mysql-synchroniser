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
package com.jbuncle.mysqlsynchroniser.structure.diff.builder;

import com.jbuncle.mysqlsynchroniser.structure.objects.Column;
import com.jbuncle.mysqlsynchroniser.util.ListUtils;
import java.util.List;
import com.jbuncle.mysqlsynchroniser.structure.diff.builder.StatementStrategy;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class ColumnStatementStrategy implements StatementStrategy<Column> {

    private final String tableName;
    private String lastColumn;

    public ColumnStatementStrategy(final String tableName) {
        this.tableName = tableName;
    }

    public String getInsertColumnAfterStatement(Column column) {
        return getAddColumnStatement(column) + " AFTER `" + lastColumn + "`;";
    }

    public String getInsertColumnFirstStatement(Column column) {
        return getAddColumnStatement(column) + " FIRST;";
    }

    private static String getNullStatement(final Column column) {
        if (column.isNullable()) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }

    private String getColumnAlterStatement(final Column column, final String action) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE `").append(tableName).append("` ").append(action);
        sb.append(" `").append(column.getColumnName()).append("`");
        sb.append(" `").append(column.getColumnName()).append("` ");
        sb.append(getColumnDefinition(column));
        return sb.toString();
    }

    private String getAddColumnStatement(Column column) {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE `").append(this.getTableName()).append("` ").append("ADD");
        sb.append(" `").append(column.getColumnName()).append("` ");

        sb.append(getColumnDefinition(column));
        return sb.toString();
    }

    private StringBuilder getColumnDefinition(Column column) {
        StringBuilder sb = new StringBuilder();
        //Type
        sb.append(column.getType()).append(" ");
        //Character set
        if (column.getCollation() != null) {
            //Collation
            sb.append("COLLATE ").append(column.getCollation()).append(" ");
        }
        //Nullable
        sb.append(getNullStatement(column)).append(" ");
        //Default
        sb.append(column.getExtra()).append(" ");
        //Comment
        sb.append("COMMENT '").append(column.getComment()).append("'");
        return sb;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public List<String> getDeleteStatement(Column column) {
//        this.lastColumn = column.getColumnName();
        return ListUtils.createListFromItem("ALTER TABLE `" + this.getTableName() + "` DROP `" + column.getColumnName() + "`;");
    }

    @Override
    public List<String> getUpdateStatement(final Column from, final Column to) {
        this.lastColumn = to.getColumnName();
        return ListUtils.createListFromItem(getColumnAlterStatement(to, "CHANGE") + ";");
    }

    @Override
    public List<String> getAddStatement(final Column column) {
        if (this.lastColumn == null) {
            this.lastColumn = column.getColumnName();
            return ListUtils.createListFromItem(this.getInsertColumnFirstStatement(column));
        } else {
            final List<String> result = ListUtils.createListFromItem(this.getInsertColumnAfterStatement(column));
            this.lastColumn = column.getColumnName();
            return result;
        }

    }

    @Override
    public void same(final Column column) {
        this.lastColumn = column.getColumnName();
    }

    @Override
    public String getKey(final Column t) {
        return t.getColumnName();
    }

}
