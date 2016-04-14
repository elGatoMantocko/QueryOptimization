package query;

import relop.Iterator;

/**
 * Created by david on 4/14/16.
 */
public class TableData {
    private final Iterator iterator;
    private final String[] names;

    public TableData(Iterator iterator, String... names) {
        this.iterator = iterator;
        this.names = names;
    }

    public String[] getNames() {
        return names;
    }

    public Iterator getIterator() {
        return iterator;
    }
}
