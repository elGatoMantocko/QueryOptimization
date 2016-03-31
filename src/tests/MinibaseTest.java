package tests;

import global.Minibase;
import global.Msql;
import heap.HeapFile;

import parser.TokenMgrError;
import parser.ParseException;

import query.QueryException;

import relop.Schema;
import relop.Tuple;
import relop.FileScan;

import query.IndexDesc;

import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;

public class MinibaseTest {

  /** Default database file name. */
  protected static String PATH = System.getProperty("user.name") + ".junit.minibase";

  /** Default database size (in pages). */
  protected static int DB_SIZE = 10000;

  /** Default buffer pool size (in pages) */
  protected static int BUF_SIZE = 100;
  
  /**Default prefetch size */
  protected static int PRE_SIZE = 10;

  /** Command line prompt, when interactive. */
  protected static String PROMPT = "\nMSQL> ";

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    // print the welcome message and load the database
    System.out.println("Minibase SQL Utility 1.0");
    if (new File(PATH).exists()) {
      // this likely wont happen because tearDown destroys the DB
      System.out.println("Loading database...");
      new Minibase(PATH, DB_SIZE, BUF_SIZE, PRE_SIZE, "Clock", true);
    } else {
      System.out.println("Creating database...");
      new Minibase(PATH, DB_SIZE, BUF_SIZE, PRE_SIZE, "Clock", false);
    }
  }

  @After
  public void tearDown() {
    // close the database and exit
    System.out.println("Destroying database...");
    Minibase.DiskManager.destroyDB();
    Minibase.DiskManager.closeDB();
  }

  @Test
  public void testCreateIndex() {
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    boolean passes = false;

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes("Students")) {
      if (desc.columnName.equals("Age") && desc.indexName.equals("IX_Age")) {
        passes = true;
      }
    }

    Assert.assertTrue("The index was not built on the correct column.", passes);
  }

  @Test
  public void testDropIndex() {
    try {
      // set up index
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    boolean passes = false;

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes("Students")) {
      if (desc.columnName.equals("Age") && desc.indexName.equals("IX_Age")) {
        passes = true;
      }
    }

    Assert.assertTrue("The index was not built on the correct column.", passes);

    try {
      Msql.execute("DROP INDEX IX_Age;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes("Students")) {
      if (desc.columnName.equals("Age") && desc.indexName.equals("IX_Age")) {
        Assert.fail("The index shouldn\'t exist.");
      }
    }
  }

  @Test
  public void testInsertGoodRow() {
    boolean passes = false;
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    Schema schema = Minibase.SystemCatalog.getSchema("Students");

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(schema, file);

    // have to do a pseudo select
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Object[] fields = t.getAllFields();
      
      if ((int)fields[0] == 1 && // sid
          ((String)fields[1]).equals("Alice") && // name
          (new Float((float)fields[2]).equals(new Float(25.67)))) { // age
        passes = true;
      }
    }

    Assert.assertTrue("No row with specified data was found.", passes);
  }

  @Test
  public void testInsertBadRow() throws QueryException {
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67, 'test');\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } finally {
      exception.expect(QueryException.class);
    }
  }
}
