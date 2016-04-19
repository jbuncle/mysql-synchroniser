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

import com.jbuncle.mysqlsynchroniser.structure.diff.builder.ColumnStatementStrategy;
import com.jbuncle.mysqlsynchroniser.structure.diff.builder.IndexStatementStrategy;
import com.jbuncle.mysqlsynchroniser.structure.diff.builder.DiffBuilder;
import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.structure.objects.Column;
import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class TableDiff {

    public static List<String> diff(final Table source, final Table target) {
        final List<String> updates = new LinkedList<>();
        updates.addAll(getColumnDiff(source, target));
        updates.addAll(getIndexDiff(source, target));
        return updates;
    }

    private static List<String> getColumnDiff(final Table source, final Table target) {
        final DiffBuilder<Column> diffBuilder = new DiffBuilder<>(new ColumnStatementStrategy(source.getTableName()));
        diffBuilder.addAllTo(source.getColumns());
        diffBuilder.addAllFrom(target.getColumns());
        return diffBuilder.generateStatements();
    }

    private static List<String> getIndexDiff(final Table source, final Table target) {
        final DiffBuilder<Index> builder = new DiffBuilder<>(new IndexStatementStrategy());
        builder.addAllTo(source.getIndexes());
        builder.addAllFrom(target.getIndexes());
        return builder.generateStatements();
    }

}
