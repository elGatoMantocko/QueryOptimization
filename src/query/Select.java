package query;

import global.*;
import heap.HeapFile;
import parser.AST_Select;
import index.HashIndex;
import relop.*;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 * Execution plan for selecting tuples.
 */
class Select extends TestablePlan {

  private String[] tables;
  private String[] cols;
  private SortKey[] orders;
  private Predicate[][] preds;
  private Schema schema;
  private boolean explain;
  private boolean distinct;

  private Iterator finalIterator;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.tables = tree.getTables();
    this.orders = tree.getOrders();
    this.preds = tree.getPredicates();
    this.cols = tree.getColumns();
    this.schema = new Schema(0);
    this.explain = tree.isExplain;
    this.distinct = tree.isDistinct;

    HashMap<String, IndexDesc> indexes = new HashMap<String, IndexDesc>();
    HashMap<String, Iterator> iteratorMap = new HashMap<String, Iterator>();
    ArrayList<Predicate[]> predsList = new ArrayList<Predicate[]>();

    for (Predicate[] p : preds) {
      predsList.add(p);
    }

    Integer[] fieldNums = new Integer[cols.length];
    // check that the predicates are valid
    try {
      // validate the query input
      for (String table : tables) {
        QueryCheck.tableExists(table);
        Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
        schema = Schema.join(tableSchema, schema);

        // this could possibly be bad if there are multiple
        //  indexes with different names that have the same column names
        for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(table)) {
          indexes.put(desc.columnName.toLowerCase(), desc);
        }

        // create a new filescan out of the current table
        HeapFile file = new HeapFile(table);
        iteratorMap.put(table, new FileScan(tableSchema, file));
      }

      QueryCheck.predicates(schema, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schema, cols[i]);
        fieldNums[i] = schema.fieldNumber(cols[i]);
      }
    } catch(QueryException e){
      for (Iterator i : iteratorMap.values()) {
        i.close();
      }
      throw e;
    }

    // for each table being joined
    for (Map.Entry<String, Iterator> entry : iteratorMap.entrySet()) {
      // save the schema for this table
      Schema tableSchema = Minibase.SystemCatalog.getSchema(entry.getKey());

      // for all of the and predicates
      for (int i = 0; i < preds.length; i++) {
        // lets build a list of all of the passable or preds
        boolean canPushSelect = true;

        // for all of the predicates in the or list
        for (Predicate pred : preds[i]) {
          // if the predicate is valid for the current table's schema
          //  add it to the list of or predicates to push the selection down
          if (canPushSelect = canPushSelect && pred.validate(tableSchema)) {
            // build keyscan on tables with indexes on a row in the pred
            //  if there is an index on the predicate's left value
            if (indexes.containsKey(((String)pred.getLeft()).toLowerCase())) {
              IndexDesc desc = indexes.get(pred.getLeft());
              HashIndex index = new HashIndex(desc.indexName);
              KeyScan scan = new KeyScan(tableSchema, index, new SearchKey(pred.getRight()), new HeapFile(desc.indexName));
              
              iteratorMap.get(entry.getKey()).close();
              iteratorMap.put(entry.getKey(), scan);
            }
          }
        }
        
        // this will build a selection using the iterator in the map
        //  if the iterator is a selection that means there is at least one and statement in the predicates
        if (canPushSelect) {
          predsList.remove(preds[i]);
          iteratorMap.put(entry.getKey(), new Selection(iteratorMap.get(entry.getKey()), preds[i]));
        }
      }
    } // push selections

    // build the finalIterator by determining join order of the iteratorMap
    System.out.println(iteratorMap);
    String[] fileNames = iteratorMap.keySet().toArray(new String[iteratorMap.size()]);
    // for each table we have to see what the join cost is for every other table

    for (int i = 0; i < fileNames.length; i++) {
      // this returns an iterator of all of the indexes on the left table
      //  with info about where it is in the table
      Selection leftIndexes = new Selection(getIndexData(), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 0, AttrType.STRING, fileNames[i]));

      int leftCount = Minibase.SystemCatalog.getRecCount(fileNames[i]);
      Schema leftSchema = Minibase.SystemCatalog.getSchema(fileNames[i]);
      for (int j = i + 1; j < fileNames.length; j++) {
        // this returns an iterator of all of the indexes on the right table
        //  with info about where it is in the table
        Selection rightIndexes = new Selection(getIndexData(), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 1, AttrType.STRING, fileNames[j]));

        int rightCount = Minibase.SystemCatalog.getRecCount(fileNames[j]);
        Schema rightSchema = Minibase.SystemCatalog.getSchema(fileNames[j]);
        System.out.println("compute cost of join " + fileNames[i] + ": " + leftCount + " " + fileNames[j] + ": " + rightCount);

        Schema joinedSchema = Schema.join(leftSchema, rightSchema);
        
        // for each of the or candidates for a join predicate
        //  we need to determine which predicate works best with this particular join
        HashMap<Predicate[], Float> candList = new HashMap<Predicate[], Float>();
        for (Predicate[] candidate : predsList) {
          boolean valid = true;
          int reduction = 1; // for now just assume cross
          for (Predicate pred : candidate) {
            // check that all of the or predicats are valid on the current join
            if (valid = valid && pred.validate(joinedSchema)) {
              // first see if there is an index on any col in the pred
              if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  (pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() == AttrOperator.EQ) {
                // find the largest reduction factor between 10, number of keys on left, and number of keys on right
                Integer[] reds = new Integer[] { new Integer(10), new Integer(getNumIndexKeys(fileNames[i])), new Integer(getNumIndexKeys(fileNames[j])) };
                reduction = Collections.max(Arrays.asList(reds));
              } else if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  !(pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() == AttrOperator.EQ) {
                // find the largest reduction factor between 2, number of keys on left, and number of keys on right
                Integer[] reds = new Integer[] { new Integer(10), new Integer(getNumIndexKeys(fileNames[i])), new Integer(getNumIndexKeys(fileNames[j])) };
                reduction = Collections.max(Arrays.asList(reds));
              } else if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  !(pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() != AttrOperator.EQ) {
                // find the largest reduction factor between 10, number of keys on left, and number of keys on right
                Integer[] reds = new Integer[] { new Integer(2), new Integer(getNumIndexKeys(fileNames[i])), new Integer(getNumIndexKeys(fileNames[j])) };
                reduction = Collections.max(Arrays.asList(reds));
              }
            }
          }

          // all of the or preds passed and we can use it to create a score
          Predicate[] truePred = new Predicate[] { new Predicate(AttrOperator.EQ, AttrType.INTEGER, 1, AttrType.INTEGER, 1) };
          if (valid) {
            candList.put(candidate, new Float((float)(leftCount * rightCount) / (float)reduction));
          } else {
            candList.put(truePred, new Float(leftCount * rightCount));
          }
        }

        // find the best predicate to join on from the list
        List<Map.Entry<Predicate[], Float>> entryList = new ArrayList<>();
        entryList.addAll(candList.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<Predicate[], Float>>() {
          @Override
          public int compare(Map.Entry<Predicate[], Float> left, Map.Entry<Predicate[], Float> right) {
            if (left.getValue() > right.getValue()) {
              return 1;
            } else if (left.getValue() < right.getValue()) {
              return -1;
            } else {
              return 0;
            }
          }
        });

        // entry list [0] should contain the most optimal predicate for the join
        //  what scenario would entryList be empty?
        for (Predicate p : entryList.get(0).getKey()) {
          System.out.print(p + " ");
        }
        System.out.println(entryList.get(0).getValue());
      }
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private int getNumIndexKeys(String fileName) {
    return 1;
  }

  private int indexCostReduction(Iterator tabIndexes, String fileName, Schema schema, Predicate pred) {
    int reduction = 10;
    if (pred.getLtype() == AttrType.COLNAME) {
      // need to reset the left indexes
      tabIndexes.restart();

      // find all indexes on the left colname
      Selection lhsIndexes = new Selection(tabIndexes, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 6, AttrType.STRING, (String)pred.getLeft()));
      if (lhsIndexes.hasNext()) {
        // there was at least one index found for that column
        //  this means that we can apply a much better reduction factor
        // TODO: we need to figure out the number of keys in that index here
      }
    }

    if (pred.getRtype() == AttrType.COLNAME) {
      // need to reset the left indexes
      tabIndexes.restart();

      // find all indexes on the right colname
      Selection rhsIndexes = new Selection(tabIndexes, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 6, AttrType.STRING, (String)pred.getLeft()));
      if (rhsIndexes.hasNext()) {
        // there was at least one index found for that column
        //  this means that we can apply a much better reduction factor
        // TODO: we need to figure out the number of keys in that index here
      }
    }

    return reduction;
  }

  private Iterator getIndexData() {
    FileScan attScan = new FileScan(Minibase.SystemCatalog.s_att, Minibase.SystemCatalog.f_att);
    FileScan indScan = new FileScan(Minibase.SystemCatalog.s_ind, Minibase.SystemCatalog.f_ind);

    SimpleJoin indexData = new SimpleJoin(attScan, indScan, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 7));

    Projection reduceCols = new Projection(indexData, 0, 1, 2, 3, 4, 5);

    return reduceCols;
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      // finalIterator.explain(0);
      // finalIterator.close();
    } else {
      // finalIterator.execute();
    }


    // System.out.println("(Not implemented)");

  } // public void execute()

} // class Select implements Plan
