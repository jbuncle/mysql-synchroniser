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

/**
 *
 * @author James Buncle
 */
public class Index {

    private final String tableName;
    private final boolean nonUnique;
    private final String keyName;
    private final String columnName;

    public Index(final String tableName, boolean nonUnique, String keyName, String columnName) {
        this.tableName = tableName;
        this.nonUnique = nonUnique;
        this.keyName = keyName;
        this.columnName = columnName;
    }

    private boolean isPrimaryKey() {
        return this.keyName.equals("PRIMARY");
    }

    private boolean isUniqueKey() {
        return !this.nonUnique;
    }

    private String getDropStatement() {
        if (isPrimaryKey()) {
            return "ALTER TABLE `" + tableName + "` DROP PRIMARY KEY;";
        } else {
            return "DROP INDEX `" + keyName + "` ON `" + tableName + "`;";
        }
    }

    private String getSetPrimaryKeyStatement() {
        return "ALTER TABLE `" + tableName + "` ADD PRIMARY KEY(`" + this.columnName + "`);";
    }

    private String getAddUniqueStatement() {
        return "ALTER TABLE `" + tableName + "` ADD UNIQUE (`" + this.columnName + "`);";
    }

    private String getAddIndex() {
        return "ALTER TABLE `" + tableName + "` ADD INDEX (`" + this.columnName + "`);";
    }

    protected String getColumnName() {
        return this.columnName;
    }

    public String getKeyName() {
        return this.keyName;
    }

    public boolean equals(Index target) {
        if (isPrimaryKey() ^ target.isPrimaryKey()) {
            return false;
        } else if (isUniqueKey() ^ target.isUniqueKey()) {
            return false;
        } else if (!this.columnName.equals(target.columnName)) {
            return false;
        }
        return true;
    }

    public String getCreateStatement() {
        if (isPrimaryKey()) {
            //Primary key
            return getSetPrimaryKeyStatement();
        } else if (isUniqueKey()) {
            //Unique key
            return getAddUniqueStatement();
        } else {
            //Just a key/index
            return getAddIndex();
        }
    }

    public String getDeleteStatament() {
        return getDropStatement();
    }
}
