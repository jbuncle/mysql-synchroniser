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
package com.jbuncle.mysqlsynchroniser.structure.objects;

import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class IndexesBuilder {

    private final String tableName;
    private final Map<String, Index> indexes;

    public IndexesBuilder(final String tableName) {
        this.indexes = new HashMap<>();
        this.tableName = tableName;
    }

    public void addIndex(final String keyName, final boolean nonUnique, final String columnName) {
        if (this.indexes.containsKey(keyName)) {
            final List<String> columnNames = this.indexes.get(keyName).getColumnNames();
            columnNames.add(columnName);
            this.indexes.put(keyName, new Index(tableName, nonUnique, keyName, columnNames));
        } else {
            final List<String> columnNames = new LinkedList<>();
            columnNames.add(columnName);
            this.indexes.put(keyName, new Index(tableName, nonUnique, keyName, columnNames));
        }
    }

    public List<Index> getIndexes() {
        return new LinkedList<>(this.indexes.values());
    }
}
