/*
 *  Copyright (c) 2013 James Buncle
 * 
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 * 
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
package com.jbuncle.mysqlsynchroniser.structure.objects;

import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;

/**
 *
 * @author James Buncle
 */
public class Table {

    private final String tableName;
    private final List<Column> columns;
    private final List<Index> indexes;

    public Table(
            final String tableName,
            final List<Column> columns,
            final List<Index> indexes) {
        this.tableName = tableName;
        this.columns = columns;
        this.indexes = indexes;
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public Index getIndex(final String keyName) {
        for (final Index index : getIndexes()) {
            if (index.getKeyName().equals(keyName)) {
                return index;
            }
        }
        return null;
    }

    public List<Index> getIndexes() {
        return this.indexes;
    }

    public Column getColumn(final String columnName) {
        for (final Column column : this.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Table other = (Table) obj;
        if (!Objects.equals(this.tableName, other.tableName)) {
            return false;
        }
        if (!CollectionUtils.isEqualCollection(this.columns, other.columns)) {
            return false;
        }
        if (!CollectionUtils.isEqualCollection(this.indexes, other.indexes)) {
            return false;
        }
        return true;
    }

}
