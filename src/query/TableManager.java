package query;

import global.Minibase;
import global.Msql;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.*;
import relop.Iterator;

import java.util.*;

/**
 * Created by david on 4/14/16.
 */
public class TableManager {
  private final List<TableData> mTables;
  Stack<TableData> mIteratorStack;
  private String[] mTableNames;

  public TableManager(String[] tables) {
    this.mTableNames = tables;
    this.mTables = new ArrayList<>();
    mTables.addAll(getTableDatas());
    this.mIteratorStack = new Stack<>();

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

  public Collection<? extends TableData> getTableDatas() {
    ArrayList<TableData> out = new ArrayList<>();

    for(String table : this.mTableNames) {
      Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
      out.add(new TableData(null, table));
    }

    return out;
  }

  public List<TableData> getTables() {
    return mTables;
  }

  public TableData getCandidate(final PredicateManager pm) {
    for (TableData data : mIteratorStack) {

      //Get tables with index equalities first, as keyscans.

      //check the indexes on the table.
      IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(data.getNames()[0]);
      if(indexes == null || indexes.length == 0) { //if no indexes
        continue; //skip, if there are no indexes, we don't want to use this table yet.
      }

      //get all predicates that qualify for an index.
      Collection<Predicate[]> predicates = pm.popIndexEqualitiesFor(data);
      if(predicates == null || predicates.size() == 0) {
        continue; //we can't use the indexes on the table.
      }

      //at this point, we have preds and indicies

      //check if preds and indicies match
      IndexDesc validIndex = null;
      Predicate[] validPredicatePair = null;

      for (Predicate[] predicatePair : predicates) {
        for (IndexDesc index : indexes) {
          for (Predicate predicate : predicatePair) {
            if (index.columnName.equals(predicate.getLeft())) { //if the colname matches
              validIndex = index;
              validPredicatePair = predicatePair;
            }
          }
        }
      }

      if(validIndex == null || validPredicatePair == null) {
        continue; //there are no valid pairings of preds to indexes.
      }

      //at this point, there is a valid index to use on this table.

      //create the keyscan.
      String tableName = data.getNames()[0]; //this only applies to tabledata that hasnt been joined. //TODO: Assert this.
      Schema tableSchema = Minibase.SystemCatalog.getSchema(tableName);
      HashIndex hashIndex = new HashIndex(validIndex.indexName);

      HeapFile heapFile = new HeapFile(tableName);

      KeyScan keyScan = new KeyScan(tableSchema, hashIndex, new SearchKey(validPredicatePair[0].getRight()), heapFile);

      mIteratorStack.remove(data);

      return new TableData(keyScan, tableName); //return a new tabledata with the keyscan and name of the table.
    }


    //if the loop completes without finding a indexable table, simply pop the stack.
    TableData pop = mIteratorStack.pop();

    if(pop.getIterator() == null) { //if the tabledata has not been initialized. (ie: first run).
      //create the filescan.
      String tableName = pop.getNames()[0]; //this only applies to tabledata that hasnt been joined. //TODO: Assert this.
      Schema tableSchema = Minibase.SystemCatalog.getSchema(tableName);
      HeapFile heapFile = new HeapFile(tableName);

      //We want to return a filescan of the table.
      pop = new TableData(new FileScan(tableSchema, heapFile), tableName);
    }
    return pop;

  }

  public boolean hasNext() {
    return mIteratorStack.size() > 0;
  }
}
