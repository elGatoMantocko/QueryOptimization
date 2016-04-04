package query;

import relop.Iterator;
import relop.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 4/4/16.
 */
public abstract class TestablePlan implements Plan {
    private Iterator final_iterator;

    protected void setFinalIterator(Iterator iter) {
        final_iterator = iter;
    }
    /**
     * Executes the plan and prints applicable output.
     */
    @Override
    abstract public void execute();

    public List<Tuple> testExecute() {
        if(final_iterator == null) throw new IllegalStateException("Set final iterator please");

        final_iterator.explain(0);

        List<Tuple> lines = new ArrayList<>();

        int cnt = 0;
        //final_iterator.getSchema().print();

        while(final_iterator.hasNext()) {
            Tuple next = final_iterator.getNext();
            lines.add(next);
            ++cnt;
        }

        final_iterator.close();
        return lines;
    }
}
