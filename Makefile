JDKPATH = /usr
LIBPATH = ./lib/bufmgr.jar:./lib/diskmgr.jar:./lib/heap.jar:./lib/index.jar:./lib/relop.jar

CLASSPATH = .:..:$(LIBPATH):$(JUNITPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = xx

all: $(PROGS)

compile:src/*/*.java
	@mkdir -p bin
	$(JAVAC) -cp $(CLASSPATH) -d ./bin ./src/*/*.java


testcompile:src/*/*.java test/*/*.java
	@mkdir -p bin
	$(JAVAC) -cp $(CLASSPATH) -d ./bin ./src/*/*.java ./test/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):./bin global.Msql queries/queries.sql

interactive: compile
	$(JAVA) -cp $(CLASSPATH):./bin global.Msql

test: testcompile
	$(JAVA) -cp $(CLASSPATH):./bin org.junit.runner.JUnitCore tests.CreateIndexTest tests.DropIndexTest tests.InsertTest tests.UpdateTest tests.DeleteTest tests.SelectTest tests.FullLifecycleTest tests.ThreeTest

clean:
	rm -rf *.minibase bin
