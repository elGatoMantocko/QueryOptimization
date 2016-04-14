package query;

import relop.Iterator;

import java.util.List;
import java.util.Stack;

/**
 * Created by david on 4/14/16.
 */
public class TableManager {
    private final List<Iterator> mTables;

    Stack<Iterator> mIteratorStack;

    public TableManager(List<Iterator> tables) {
        this.mIteratorStack = new Stack<>();
        this.mIteratorStack.addAll(tables);

        this.mTables = tables;
    }

    public List<Iterator> getTables() {
        return mTables;
    }


    public Iterator getCandidate() {
        return mIteratorStack.pop();
    }

    public boolean hasNext() {
        return mIteratorStack.size() > 0;
    }
}
