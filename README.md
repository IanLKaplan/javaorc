# javaorc
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
Double    | Double
byte[]    | Bytes
String    | Bytes
BigDecimal | Decimal
java.sql.Timestamp | Timestamp
List<Object> | List
List<Object> | Struct
Map<Object, Object> | Map
List<Pair<TypeDescription, Object> | Union

## References
The intent of the javaorc code is to abstract the internal structures needed two write and read ORC files into a simple interface. If you would like to delve into the javaorc code the referneces below are useful in explaining the ORC format.

* Apache ORC types: https://orc.apache.org/docs/types.html
* Apache ORC documentation: https://orc.apache.org/docs/core-java.html
* ORC Core: https://javadoc.io/doc/org.apache.orc/orc-core/latest/index.html
* HIVE Storage API: https://orc.apache.org/api/hive-storage-api/index.html
