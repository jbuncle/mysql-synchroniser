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

import com.jbuncle.mysqlsynchroniser.structure.objects.Column;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class ColumnStatementBuilder {

    private final String tableName;
    private final Column column;

    public ColumnStatementBuilder(final String tableName, final Column column) {
        this.tableName = tableName;
        this.column = column;
    }

    public String getDeleteStatement() {
        return "ALTER TABLE `" + this.getTableName() + "` DROP `" + this.getColumn().getColumnName() + "`;";
    }

    public String getInsertColumnAfterStatement(final Column after) {
        return getAddColumnStatement() + " AFTER `" + after.getColumnName() + "`;";
    }

    public String getInsertColumnFirstStatement() {
        return getAddColumnStatement() + " FIRST;";
    }

    private static String getNullStatement(final Column column) {
        if (column.isNullable()) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }

    private String getColumnAlterStatement(final String action) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE `").append(tableName).append("` ").append(action);
        sb.append(" `").append(column.getColumnName()).append("`");
        sb.append(" `").append(column.getColumnName()).append("` ");
        sb.append(getColumnDefinition());
        return sb.toString();
    }

    private String getAddColumnStatement() {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE `").append(this.getTableName()).append("` ").append("ADD");
        sb.append(" `").append(this.getColumn().getColumnName()).append("` ");

        sb.append(getColumnDefinition());
        return sb.toString();
    }

    private StringBuilder getColumnDefinition() {
        StringBuilder sb = new StringBuilder();
        //Type
        sb.append(this.getColumn().getType()).append(" ");
        //Character set
        if (this.getColumn().getCollation() != null) {
            //Collation
            sb.append("COLLATE ").append(this.getColumn().getCollation()).append(" ");
        }
        //Nullable
        sb.append(getNullStatement(this.getColumn())).append(" ");
        //Default
        sb.append(this.getColumn().getExtra()).append(" ");
        //Comment
        sb.append("COMMENT '").append(this.getColumn().getComment()).append("'");
        return sb;
    }

    public String getUpdateStatement() {
        return getColumnAlterStatement("CHANGE") + ";";
    }

    public Column getColumn() {
        return column;
    }

    public String getTableName() {
        return tableName;
    }

}
