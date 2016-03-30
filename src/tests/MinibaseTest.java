package tests;

import global.Minibase;

import parser.AST_Start;
import parser.MiniSql;
import parser.ParseException;
import parser.TokenMgrError;
import query.Optimizer;
import query.Plan;
import query.QueryException;

import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Assert;
import org.junit.Test;

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

  private int allocs;
  private int reads;
  private int writes;

  private AST_Start node;

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

    // initialize the performance counters
    allocs = Minibase.DiskManager.getAllocCount();
    reads = Minibase.DiskManager.getReadCount();
    writes = Minibase.DiskManager.getWriteCount();
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
    // TODO: Set system.in to some InputStream and send that to the parser
    // TODO: Create a table and create an index on that table (similar to queries.sql)
    // TODO: Verify that the index has been created
    MiniSql parser;
  }

  @Test @Ignore
  public void testDropIndex() {
    // TODO: Set system.in to some InputStream and send that to the parser
    // TODO: Create a table and create an index on that table (similar to queries.sql)
    // TODO: Drop that index
    // TODO: Verify that the index has been dropped
  }
}
