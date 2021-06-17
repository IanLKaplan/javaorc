# javaorc
### by Ian Kaplan, www.topstonesoftware.com, iank@bearcave.com
Javaorc is a Java library that makes writing and reading ORC files easy.

## Introduction
The Optimized Row Columnar (ORC) file format was originally developed for the [Apache Hive](https://hive.apache.org) data warehouse. The ORC format is also used by a number of other data warehouses, including the [Amazon Web Services Athena](https://aws.amazon.com/athena) database and the [Snowflake](https://www.snowflake.com/) data warehouse.

The ORC format supports the creation of large compressed files with a columnar structure. ORC files can serve as logical proxies for relational tables for a system like AWS Athena or Hive. Storing data in ORC files can dramatically increase the performance of data query operations on systems like Athena, where the data that is being queried is stored on AWS S3.

The javaorc library was developed to make writing and reading ORC files simple. With javaorc the programmer does not have to master the details of the ORC data structures.

## Maven Reference
The javaorc jar file has been uploaded to Maven Central.  To use the pre-built jar add the below to your pom.xml file

```
        <dependency>
            <groupId>com.topstonesoftware.javaorc</groupId>
            <artifactId>javaorc</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>`

```

## ORC File Structure
An ORC file consists of a set of one or more columns. Each column has a defined element type. The ORC file format is a hierarchical format that supportes both atomic (Integer, Long, Double) and structured (arrays, structures, maps and unions) column elements.

## ORC File Schema
The columnar structure of an ORC file is defined by a schema. The schema is constructed with a hierarchy of [TypeDescription]( https://orc.apache.org/api/orc-core/org/apache/orc/TypeDescription.html) objects from the orc-core API. For example:

       TypeDescription schema = TypeDescription.createStruct();
       schema.addField("symbol", TypeDescription.createString());
       schema.addField("close", TypeDescription.createDouble());
       schema.addField("date", TypeDescription.createTimestamp());
       schema.addField("shortable", TypeDescription.createBoolean());
       
This schema definition will create an ORC file with four labeled columns: 
1. symbol : a String column 
1. close : a double column 
1. date : a timestamp column 
1. shortable : a boolean columm

The schema can be printed (via println() or converted to a String):

```
struct<symbol:string,close:double,date:timestamp,shortable:boolean>
```
## The WriteORCFile object

The _javaorc_ ```WriteORCFile``` object implements the Autoclosable interface. When used in a try block the object will automatically be closed, freeing memory resources.

The WRiteORCFile object is initialized with a String that defines the file path and a ```TypeDescription``` object that defines the ORC file schema.

```
      try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            for (var i = 0; i < NUM_ROWS; i++) {
                row.clear();
                ...
                orcWriter.writeRow( row );
            }
      }
```

The ```WriteORCFile``` method ```writeRow()``` is passed a list of Java Objects (for example, an ```ArrayList<Object>```).  Each element of the List is written to the associated column.

## Column Types

The following types are supported by _javaorc_

Java Type | ORC Column Type
----------|----------------
Integer   | Long
Long      | Long
Boolean   | Long
Double    | Double
byte[]    | Bytes
String    | Bytes
BigDecimal | Decimal
java.sql.Timestamp | Timestamp
List<Object> | List
List<Object> | Struct
Map<Object, Object> | Map
List<Pair<TypeDescription, Object> | Union
 
There appears to be a bug in the way the ORC core code handles DateColumnVector values. The values are written as long epoch date values, but when the value is read a 32-bit values is read from the ORC file, yielding an incorrect date.  A java.sql.Timestamp value should be used instead of a java.util.Date value.
       
### Example:
       
The schema below defines an ORC file with three columns:
1. A String column with the label 'symbol'
1. A Double column with the label 'close'
1. A Timestamp column with the label 'date'
       
```
       TypeDescription schema = TypeDescription.createStruct();
       schema.addField("symbol", TypeDescription.createString());
       schema.addField("close", TypeDescription.createDouble());
       schema.addField("date", TypeDescription.createTimestamp());
```

A ```List<Object>``` object is used to write an ORC file row:

```
   List<Object> row = new ArrayList<>();
   ...
   String symbol = /* initialize with a string value */
   Double close = /* initialize with a double value */
   Timestamp date = new Timestamp( date.getTime() );
   row.add(symbol);
   row.add(close);
   row.add(date);
   orcWriter.writeRow( row );
   
```
## List Column Elements
An ORC file may have column elements that consists of variable length lists.
       
The schema below defines an ORC file with a single column, where each column element is a list.
       
In the example below three ORC file rows are written out for the single column ORC file. The first row has a list element that consists of four Long values, the second row has two values and the third row has five values.
       
```
        TypeDescription schema = TypeDescription.createStruct();
        TypeDescription longListType = TypeDescription.createList(TypeDescription.createLong());
        schema.addField("long_list", longListType);

       try(var orcWriter = new WriteORCFile(filePathStr, schema)) { 
           List<Object> row = new ArrayList<>();
           row.add( Arrays.asList(new Long[]{1L, 2L, 3L, 4L}) );
           orcWriter.writeRow( row );
           row.clear();
           row.add( Arrays.asList(new Long[]{5L, 6L}) );
           orcWriter.writeRow( row );
           row.clear();
           row.add( Arrays.asList(new Long[]{7L, 8L, 9L, 10L, 11L}) );
           orcWriter.writeRow( row );
       }
```
The types for a list are limited to:
* Integer
* Long
* Boolean
* Double
* byte[]
* String
* BigInteger
* Timestamp
       
## Struct Column Elements
An ORC List column consists of Lists where all elements of the list are the same type.  An ORC Struct column is like a C/C++ struct object were there are multiple named fields, possibly with different types. 
       
The schema below defines an ORC struct column element with three fields.
       
```
        TypeDescription schema = TypeDescription.createStruct();
        TypeDescription structDef = TypeDescription.createStruct();
        structDef.addField("word", TypeDescription.createString());
        structDef.addField("word_hash", TypeDescription.createInt());
        structDef.addField("hash_is_odd", TypeDescription.createBoolean());
        schema.addField("structCol", structDef);
```
The three Objects that make up the struct column value are stored in a List<Object>.  Unlike a List column element, each element in the list may have a different type
       
```
       List<Object> row = new ArrayList<>();
       List<Object> fieldList = new ArrayList<>();
       fieldList.add(word);
       fieldList.add(word.hashCode());
       fieldList.add( (word.hashCode() & 0x1) == 1 ? Boolean.TRUE : Boolean.FALSE );
       row.add(fieldList);
       orcWriter.writeRow(row);
```
## Map Column Element
       
The ORC format supports map column elements (e.g., key, value pairs).  The key types are limited to:
       
* Long
* String
* Double

The map values are limted to:
       
* Long
* String
* Double
* BitInteger
* Timestamp
       
The schema definition below has a single map column with a String key and an Integer value.

```
       TypeDescription schema = TypeDescription.createStruct();
       TypeDescription map = TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createInt());
       schema.addField("word_freq", map);
```
The map column element value is taken from a Java Map element (in this case ```HashMap<String, Integer>```)       
```
       HashMap<String, Integer> wordFreqMap = new HashMap<>();
       ...
       List<Object> row = new ArrayList<>();
       row.add( wordFreqMap);
       orcWriter.writeRow( row );
```
The map column type might be used when there is a set of maps, one for each column element. Each map element in a column must have the same type (e.g., the same key and value type). These map column values could be associated with other column value. For example, a document name could be associated with a map of word frequency key/value pairs. This could be used to build [TF/IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) values for a document set.
       
## Union Column Values
       
A union column value allows a column to have more than one type. For example, a column could store Long, Double and String values.  Unlike a union in the C programming language, a row column element only stores a single value type.
       
The Java code below will create a schema for an ORC file with a single column, with the field name "union".
       
```
       TypeDescription schema = TypeDescription.createStruct();
       TypeDescription union = TypeDescription.createUnion();
       union.addUnionChild( TypeDescription.createInt());
       union.addUnionChild( TypeDescription.createTimestamp());
       union.addUnionChild( TypeDescription.createString());
       schema.addField("union", union);
```
This union defined in this schema can store row element values with Integer, Timestamp or String values.
       
To avoid ambiguity, the values written to a union type column are written as a ```Pair<TypeDescription, Object>```. For example:
       
```
       List<Object> row = new ArrayList<>();
       ...
       Object intObj = an integer value
       Pair<TypeDefinition, Object> intUnionValue = new ImmutablePair<>(unionFieldType, intObj);
       row.add( intUnionValue );
       ...
       orcWriter.writeRow( row );
```

## The ReadORCFile object 
       
The ReadORCFile object supports reading ORC files. In the _javaorc_ library this class supports the WriteORCFile unit tests.
       
Like the WriteORCFile object, the ReadORCFile object implements the AutoClosable interface allowing it to be used within a ```try``` block.
       
The code below is a skeleton for code that will read all of the rows within the ORC file.  The ```ReadORCFile``` constructor is passed the path to the ORC file being read.
       
The ReadORCFile readRow() method returns a List<Object> object.  Each element in this object corresponds to a column in the ORC file.
       
```
    try(var orcReader = new ReadORCFile(filePathStr)) {
       ...
        List<Object> row;
        while ((row = orcReader.readRow()).size() > 0) {
             ...
        }
    }
```
The schema for the ORC file being read is returned by the getSchema() method:
       
```
       TypeDescription schema = orcReader.getSchema();
```
   
## setORCWriter: providing a custom ORC file writer for AWS S3
       
The _javaorc_ library is designed to make writing ORC files for cloud resources, like AWS Athena, easy.  The work flow might be:
       
Read files from AWS S3 ==> Write ORC Files ==> Query ORC file data via Athena SQL
       
In this work flow the S3 file reader and the code that writes the ORC file could be resident on a component in the AWS cloud. To support this, an ORC file writer that can write to S3 would be needed.
      
Both the ```WriteORCFile``` and ```ReadORCFile``` objects provide public methods that allow a custom ORC file writer or reader to be provided via ```setORCWriter()``` and ```setORCReader()``` methods, respectively.
       
The code below outlines how a custom S3 ORC file writer could be set for the WriteORCFile object. Note that the writer is set before the ```writeRow()``` method is called.
       
```
       try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
           orcWriter.setORCWriter( myS3FileORCWriter );
           ...
           orcWriter.writeRow( row );
       }
```
### The S3AFileSystem FileSystem object
       
In the Hadoop ecosystem there has been some work done on creating an S3 FileSystem object that can be used to build an ORC file Writer object. 
       
The FileSystem here is a Hadoop FileSystem object ```org.apache.hadoop.fs.FileSystem``` not the Java ```java.nio.file.FileSystem```  While these two objects share a common class name, they do not share an object hierarchy.
       
The code below will create a Hadoop FileSystem object that can write to AWS S3.  The ```fileNamePath``` is the file name in the S3 bucket (e.g., myfile.orc).
The ```s3Bucket argument is the name of the S3 bucket.
       
```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
  
 /*
      Return an ORC file Writer object that can write to the S3 file system.
   */
public Writer buildS3OrcWriter(String fileNamePath, TypeDescription schema, String s3Bucket) throws IOException, URISyntaxException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = new S3AFileSystem();
    String uriStr = "s3://" + s3Bucket;
    fileSystem.initialize(new URI(uriStr), configuration);
    return OrcFile.createWriter(new Path(fileNamePath),
            OrcFile.writerOptions(configuration)
                    .fileSystem(fileSystem)
                    .setSchema(schema)
                    .overwrite(true));
}
```
#### References

*   S3A docs https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html 
*   hadoop-aws API https://javadoc.io/doc/org.apache.hadoop/hadoop-aws/latest/index.html
*   S3A https://javadoc.io/static/org.apache.hadoop/hadoop-aws/3.3.0/org/apache/hadoop/fs/s3a/S3A.html
*   S3AFileSystem https://javadoc.io/static/org.apache.hadoop/hadoop-aws/3.3.0/org/apache/hadoop/fs/s3a/S3AFileSystem.html
*   Hadoop S3A trouble shooting: https://hadoop.apache.org/docs/r3.1.1/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html
*   Configuration https://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html

#### Maven Dependencies
        
The S3AFileSystem requires the Hadoop includes below.  To avoid an undefined class error at runtime, the Jackson library just be included.
        
```
    <properties>
        <hadoop.version>3.3.0</hadoop.version>
    </properties>
        
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <version>2.12.3</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-aws</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
```

#### AWS S3 Authentication
        
The S3AFileSystem must authenticate with AWS S3.  Credentials for AWS S3 authentication can be provided in the ~/.aws/credentials file or as environment variables.  I generally set the credentials as environment varibles in the ```.bashrc`` file.
                
```
AWS_ACCESS_KEY_ID= AWS S3 access key
AWS_SECRET_ACCESS_KEY= AWS S3 secret key

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
```

#### Writing to S3
        
I have successfully used the Writer object built by ```buildS3OrcWriter()``` to initialize a WriteORCFile object and write to an S3 bucket.  The file (the ```fileNamePath``` argument) will be written to ```mybucket/user/myUserName``` (for example, ```myorcbucket/user/iank```)  I would prefer to omit the user/myUserName prefix, but I have not succeeded in doing this with a call to ```  fileSystem.setWorkingDirectory()```

        
## Some comments
       
The column and element structure that can be defined for an ORC file allows highly complex ORC files to be defined. ORC files are often targeted at SQL engines like Athena or Hive. While vector (array) and struct elements are supported in SQL queries, these queries may not be as efficient as queries on atomic elements (e..g, Integer, String, Double).  
       
Apparently Athena does support queries on maps (see [_The Athena Guide: Working with complex types_ by Theo Tolv](https://athena.guide/articles/complex-types/))
       
```
SELECT
  params['id'] AS id,
  params['value'] AS value
FROM my_table
```
The complex nature of ORC files and the vast number of combinations that can be used to create a schema makes the _javaorc_ code difficult to exhaustivelyh test.  There are not tests (yet) that explore the limits of the size of column elements. For example, column elememnts with 20K vectors or maps.  The usefulness of such column elements seems questionable, so it may not be a problem that these tests have not been written.
       
### Java Modularity
Summary: Java modules are useless in many cases. Java modules require that a component be defined in exactly one place. Modules fail if the Java code uses more than one jar that includes the same component.
       
The _javaorc_ library depends on orc, hive and hadoop jar files.  These jars are an "everything including the kitchen sink" collection of sub-components like the jetty web server.  I have tried to exclude the components that are not used in _javaorc_ in the pom.xml file. This is still a less than ideal situation since there may still be library components that interact with the environment that includes the _javaorc_ code.
       
What would be great is to have an infrastructure that would only export the three _javaorc_ classes and keep everything else, like random web servers, behind a wall that would not link with components outside of _javaorc_.  Java 9 introduced the module feature, so I thought that perhaps this was exactly what I was looking for.  Sadly, this turned out to not be the case.
       
The module definition in src/main/java/module-info.java was:
       
```
 module com.topstonesoftware.javaorc {
    requires org.apache.commons.lang3;
    requires java.sql;
    requires orc.core;
    requires hadoop.common;
    requires hadoop.hdfs.client;
    requires hive.storage.api;
    exports com.topstonesoftware.javaorc;
}
       
 ```
 The code compiled, but when I tried to run it, I ran into JVM execution errors.  These were the result of the infamous split library problem, where a component is included in more than one library.  The runtime errors are:
  
```
the unnamed module reads package org.apache.hadoop.fs from both hadoop.common and hadoop.hdfs.client
module hadoop.common reads package org.apache.hadoop.fs from both hadoop.hdfs.client and hadoop.common
module orc.core reads package org.apache.hadoop.fs from both hadoop.hdfs.client and hadoop.common
module hadoop.hdfs.client reads package org.apache.hadoop.fs from both hadoop.hdfs.client and hadoop.common
module hive.storage.api reads package org.apache.hadoop.fs from both hadoop.hdfs.client and hadoop.common
module org.apache.commons.lang3 reads package org.apache.hadoop.fs from both hadoop.hdfs.client and hadoop.common
```
Other than cleaning up libraries (which is not possible when using third party libraries), there seems to be no solution to the split library problem.
       
Legacy libraries often include components that are also included in other jars that provide neccessary software.  The strict insistence in the Java module feature that a component be provided by one and only one jar means that Java modules are useless for _javaorc_ and, I suspect, many other software components.
       
## Test Code
       
Tests have been written for each of the ORC column elements. These tests can provide a reference for writing and reading ORC files.
       
## ORC References
              
The intent of the javaorc code is to abstract the internal structures needed two write and read ORC files into a simple interface. If you would like to delve into the javaorc code the referneces below are useful in explaining the ORC format.

* Apache ORC types: https://orc.apache.org/docs/types.html
* Apache ORC documentation: https://orc.apache.org/docs/core-java.html
* ORC Core: https://javadoc.io/doc/org.apache.orc/orc-core/latest/index.html
* HIVE Storage API: https://orc.apache.org/api/hive-storage-api/index.html
       
## Athena References
* [The Athena Guide](https://athena.guide/) by Theo Tolv
* [AWS Athena documentation](https://docs.aws.amazon.com/athena/index.html)
