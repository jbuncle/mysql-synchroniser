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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 * @param <T>
 */
public class DiffBuilder<T> {

    private final Map<String, T> from;
    private final Map<String, T> to;
    private final StatementStrategy<T> statementBuilder;

    public DiffBuilder(StatementStrategy<T> scriptGenerator) {
        this.from = new LinkedHashMap<>();
        this.to = new LinkedHashMap<>();
        this.statementBuilder = scriptGenerator;
    }

    public void addAllFrom(Iterable<T> items) {
        for (T item : items) {
            this.addFrom(this.statementBuilder.getKey(item), item);
        }
    }

    public void addAllTo(Iterable<T> items) {
        for (T item : items) {
            this.addTo(this.statementBuilder.getKey(item), item);
        }
    }

    public void addFrom(final String key, final T object) {
        this.from.put(key, object);
        if (!this.to.containsKey(key)) {
            this.to.put(key, null);
        }
    }

    public void addTo(final String key, final T object) {
        this.to.put(key, object);
        if (!this.from.containsKey(key)) {
            this.from.put(key, null);
        }
    }

    public List<String> generateStatements() {
        final List<String> diffs = new LinkedList<>();
        for (final String key : from.keySet()) {
            final T from = this.from.get(key);
            final T to = this.to.get(key);
            if (from == null && to != null) {
                diffs.addAll(this.statementBuilder.getAddStatement(to));
            } else if (from != null && to == null) {
                diffs.addAll(this.statementBuilder.getDeleteStatement(from));
            } else if (!from.equals(to)) {
                diffs.addAll(this.statementBuilder.getUpdateStatement(from, to));
            } else {
                this.statementBuilder.same(to);
            }
        }
        return diffs;
    }

}
