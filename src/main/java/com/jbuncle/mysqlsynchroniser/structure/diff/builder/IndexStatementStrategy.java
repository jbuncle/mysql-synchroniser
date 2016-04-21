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

import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import com.jbuncle.mysqlsynchroniser.util.ListUtils;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class IndexStatementStrategy implements StatementStrategy<Index> {

    @Override
    public List<String> getDeleteStatement(Index t) {
        return ListUtils.createListFromItem(t.getDeleteStatament());
    }

    @Override
    public List<String> getUpdateStatement(Index from, Index to) {
        List<String> list = new LinkedList<>();
        list.add(from.getDeleteStatament());
        list.add(to.getCreateStatement());
        return list;
    }

    @Override
    public List<String> getAddStatement(Index t) {
        return ListUtils.createListFromItem(t.getCreateStatement());
    }

    @Override
    public void same(Index t) {
        //Nothing to do for no changes
    }

    @Override
    public String getKey(Index t) {
        return t.getKeyName();
    }

}
