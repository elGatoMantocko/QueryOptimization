package tests;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import global.Minibase;
import global.Msql;

import heap.HeapFile;

import relop.Tuple;
import relop.FileScan;

import parser.ParseException;
import parser.TokenMgrError;

import query.QueryException;

public class DeleteTest extends MinibaseTest {

  @Before
  public void setUp() {
    super.setUp();

    try {
      // create table
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nQUIT;");
      // build table
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nINSERT INTO Students VALUES (2, 'Chris', 12.34);\nINSERT INTO Students VALUES (3, 'Bob', 30.0);\nINSERT INTO Students VALUES (4, 'Andy', 50.0);\nINSERT INTO Students VALUES (5, 'Ron', 30.0);\nQUIT");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDeleteOneRow() {
    // delete row
    try {
      Msql.execute("DELETE Students WHERE name = 'Chris';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Assert.assertNotEquals("Row with name 'Chris' wasn't deleted", "Chris", t.getField("name"));
    }

    scan.close();
  }

  @Test
  public void testDeleteMultiRow() {
    try {
      Msql.execute("DELETE Students WHERE name = 'Chris' or name = 'Alice';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Assert.assertNotEquals("Row with name 'Chris' wasn't deleted", "Chris", t.getField("name"));
      Assert.assertNotEquals("Row with name 'Alice' wasn't deleted", "Alice", t.getField("name"));
    }

    scan.close();
  }

  @Test
  public void testNoRowsToDelete() {
    try {
      Msql.execute("DELETE Students WHERE name = 'Elliott';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);
    int rowCount = 0;

    while (scan.hasNext()) {
      scan.getNext();
      rowCount++;
    }

    scan.close();

    Assert.assertEquals("The number of rows didn't change", 5, rowCount);
  }

  @Test (expected=QueryException.class)
  public void testDeleteTableDoesntExist() throws QueryException {
    try {
      Msql.execute("DELETE Bad;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }

  @Test (expected=QueryException.class)
  public void testDeletePredicatesInvalid() throws QueryException {
    try {
      Msql.execute("DELETE Students WHERE bad = 'Elliott';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }
}
