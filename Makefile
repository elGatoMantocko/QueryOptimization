JDKPATH = /usr
LIBPATH = ./lib/bufmgr.jar:./lib/diskmgr.jar:./lib/heap.jar:./lib/index.jar:./lib/relop.jar
JUNITPATH = /Applications/IntelliJ\ IDEA\ 15.app/Contents/lib/junit-4.12.jar
HAMCRESTPATH = /Applications/IntelliJ\ IDEA\ 15.app/Contents/lib/hamcrest-core-1.3.jar

CLASSPATH = .:..:$(LIBPATH):$(JUNITPATH):$(HAMCRESTPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = xx

all: $(PROGS)

compile:src/*/*.java
	@mkdir -p bin
	$(JAVAC) -cp $(CLASSPATH) -d ./bin ./src/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):./bin global.Msql queries/queries.sql

test: compile
	$(JAVA) -cp $(CLASSPATH):./bin org.junit.runner.JUnitCore tests.CreateIndexTest tests.DropIndexTest tests.InsertTest tests.UpdateTest tests.DeleteTest tests.SelectTest

clean:
	rm -rf *.minibase bin
