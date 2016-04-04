package query;

import relop.Iterator;
import relop.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 4/4/16.
 */
public abstract class TestablePlan implements Plan {
    Iterator final_iterator;

    /**
     * Executes the plan and prints applicable output.
     */
    @Override
    abstract public void execute();

    public List<Tuple> testExecute() {
        final_iterator.explain(0);

        List<Tuple> lines = new ArrayList<>();

        int cnt = 0;
        final_iterator.getSchema().print();

        while(final_iterator.hasNext()) {
            Tuple next = final_iterator.getNext();
            lines.add(next);
            ++cnt;
        }

        final_iterator.close();
        return lines;
    }
}
