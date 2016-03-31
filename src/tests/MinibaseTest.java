package tests;

import global.Minibase;

import global.Msql;

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
    // TODO: Verify that the index has been created
    // set up index
    Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
  }

  @Test
  public void testDropIndex() {
    // TODO: Verify that the index has been dropped
    // set up index
    Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");

    // drop index
    Msql.execute("DROP INDEX IX_Age;\nQUIT;");
  }
}
