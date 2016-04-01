package tests;

import org.junit.Test;
import org.junit.Before;

import global.Msql;
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
  }
  
}