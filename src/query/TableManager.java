package query;

import global.Minibase;
import global.Msql;
import relop.Iterator;

import java.util.*;

/**
 * Created by david on 4/14/16.
 */
public class TableManager {
  private final List<TableData> mTables;

  Stack<TableData> mIteratorStack;

  public TableManager(List<TableData> tables) {
    this.mIteratorStack = new Stack<>();

    this.mTables = new ArrayList<>();
    mTables.addAll(tables);
    Collections.sort(this.mTables, new Comparator<TableData>() {
      @Override
      public int compare(TableData l, TableData r) {
        int ltablecost = 1;
        for (String ltable : l.getNames()) {
          ltablecost *= Minibase.SystemCatalog.getRecCount(ltable);
        }

        int rtablecost = 1;
        for (String rtable : r.getNames()) {
          rtablecost *= Minibase.SystemCatalog.getRecCount(rtable);
        }

        return rtablecost - ltablecost ;
      }
    });

    this.mIteratorStack.addAll(mTables);

  }

  public List<TableData> getTables() {
    return mTables;
  }


  public TableData getCandidate() {
    return mIteratorStack.pop();
  }

  public boolean hasNext() {
    return mIteratorStack.size() > 0;
  }
}
