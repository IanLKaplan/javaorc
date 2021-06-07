# javaorc
### by Ian Kaplan, www.topstonesoftware.com, iank@bearcave.com
A Java library that makes writing and reading ORC files easy.

## Introduction
The Optimized Row Columnar (ORC) file format was originally developed for the [Apache Hive](https://hive.apache.org) data warehouse. The ORC format is also used by a number of other data warehouses, including the [Amazon Web Services Athena](https://aws.amazon.com/athena) database and the [Snowflake](https://www.snowflake.com/) data warehouse.

The ORC format supports the creation of large compressed files with a columnar structure. ORC files can serve as logical proxies for relational tables for a system like AWS Athena or Hive. Storing data in ORC files can dramatically increase the performance of data query operations on systems like Athena, where the data that is being queried is stored on AWS S3.

The javaorc library was developed to make writing and reading ORC files simple. With javaorc the programmer does not have to master the details of the ORC data structures.

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
The map column type might be used when there is a set of maps, one for each column element. Each map element in a column must have the same type (e.g., the same key and value type). These map column values could be associated with other column value. For example, the name associaed with a given map.
       
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
