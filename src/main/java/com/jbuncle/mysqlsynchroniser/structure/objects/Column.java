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
 * 
 */
package com.jbuncle.mysqlsynchroniser.structure.objects;

import java.util.Objects;

/**
 *
 * @author James Buncle
 */
public class Column {

    private final String columnName;
    private final String type;
    private final boolean nullable;

    private final String key;
    private final String defaultValue;
    private final String extra;
    private final String collation;
    private final String comment;

    public String getName() {
        return columnName;
    }

    public Column(
            final String field,
            final String type,
            final boolean nullable,
            final String key,
            final String defaultValue,
            final String extra,
            final String collation,
            final String comment) {

        this.columnName = field;
        this.type = type;
        this.nullable = nullable;
        this.key = key;
        this.defaultValue = defaultValue;
        this.extra = extra;
        this.collation = collation;
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "MySQLTableDescription{" + "field=" + columnName + ", type=" + type + ", nullable=" + nullable + ", key=" + key + ", defaultValue=" + defaultValue + ", extra=" + extra + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) {
            return false;
        }
        final Column other = (Column) obj;
        if (this.nullable != other.nullable) {
            return false;
        }
        if (!Objects.equals(this.columnName, other.columnName)) {
            return false;
        }

        if (!Objects.equals(this.type, other.type)) {
            return false;
        }

        if (!Objects.equals(this.key, other.key)) {
            return false;
        }

        if (!Objects.equals(this.defaultValue, other.defaultValue)) {
            return false;
        }
        if (!Objects.equals(this.extra, other.extra)) {
            return false;
        }
        if (!Objects.equals(this.collation, other.collation)) {
            return false;
        }
        return Objects.equals(this.comment, other.comment);
    }

    public String getColumnName() {
        return columnName;
    }

    public String getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getExtra() {
        return extra;
    }

    public String getCollation() {
        return collation;
    }

    public String getComment() {
        return comment;
    }

}
