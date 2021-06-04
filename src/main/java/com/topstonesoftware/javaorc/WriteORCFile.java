/*
 * Ian L. Kaplan (iank@bearcave.com) licenses this software
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.topstonesoftware.javaorc;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * <h3>
 *  WriteORCFile
 * </h3>
 * <p>
 *     A class that supports writing files in ORC file format.
 * </p>
 * <p>
 *     A list of Objects is written to the ORC file for each row. The ORC file schema passed to the class constructor
 *     is used to interpret how each of the Object values should be cast and written to the ORC file.
 * </p>
 * <p>
 *     One instance of this class is allocated for each ORC file that is written.  The class has internal state cannot be
 *     reused and must be allocated for each file.
 * </p>
 * <p>
 *     This class is not thread safe.
 * </p>
 * <h3>
 * References
 * </h3>
 * <h4>
 * Code Examples
 * </h4>
 * <ul>
 *     <li>A code example showing schema construction: https://www.jianshu.com/p/a9f8b7aa3c28</li>
 *     <li>A code example showing how to write a string to ORC format: https://www.javahelps.com/2020/08/read-and-write-orc-files-in-core-java.html</li>
 *     <li>A simple JSON reader that populates ColumnVector objects: <br/>
 *     https://github.com/apache/orc/blob/main/java/tools/src/java/org/apache/orc/tools/convert/JsonReader.java</li>
 *     <li>ORC file format overview: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC</li>
 *     <li>ORC V1 file format: https://orc.apache.org/specification/ORCv1/</li>
 * </ul>
 * <h4>
 *     API
 * </h4>
 * <ul>
 *     <li>Apache ORC types: https://orc.apache.org/docs/types.html</li>
 *     <li>Apache ORC documentation: https://orc.apache.org/docs/core-java.html</li>
 *     <li>ORC Core: https://javadoc.io/doc/org.apache.orc/orc-core/latest/index.html</li>
 *     <li>HIVE Storage API: https://orc.apache.org/api/hive-storage-api/index.html</li>
 *     <li>HIVE Storage API source on GitHub: https://github.com/apache/hive/tree/master/storage-api/src/java/org/apache/hadoop/hive/ql/exec/vector</li>
 *     <li>Hadoop file system (3.3.0): https://hadoop.apache.org/docs/r3.3.0/api/index.html</li>
 * </ul>
 *
 * <h4>
 *     Downloads
 * </h4>
 * <ul>
 *     <li>https://mirror.olnevhost.net/pub/apache/orc/</li>
 * </ul>
 *
 * @author Ian Kaplan, Topstone Software, www.topstonesoftware.com and www.bearcave.com
 * iank@bearcave.com
 *
 */
public class WriteORCFile extends ORCFileIO implements AutoCloseable {
    private static final Predicate<Object> isInteger = Integer.class::isInstance;
    private static final Predicate<Object> isLong = Long.class::isInstance;
    private static final Predicate<Object> isDouble = Double.class::isInstance;
    private static final Predicate<Object> isString = String.class::isInstance;
    private static final Predicate<Object> isBigDecimal = BigDecimal.class::isInstance;
    private static final Predicate<Object> isDate = Date.class::isInstance;

    Writer orcWriter = null;
    final TypeDescription schema;
    final String fileNamePath;
    final VectorizedRowBatch batch;
    final List<String> fieldNames;

    /**
     * @param filePath the path to a file in the local file system.
     * @param schema the ORC schema
     */
    WriteORCFile(String filePath, TypeDescription schema) {
        this.fileNamePath = filePath;
        this.schema = schema;
        batch = this.schema.createRowBatch();
        fieldNames = schema.getFieldNames();
    }

    /**
     * Set the ORC writer. This method can be used to replace the default local file system writer with a writer
     * for another file system (for example, a writer for AWS S3).
     *
     * @param orcWriter The Writer object that should be used when writing the ORC file.
     */
    public void setOrcWriter(Writer orcWriter) {
        this.orcWriter = orcWriter;
    }

    private Writer buildOrcWriter() throws ORCFileException {
        Writer writer;
        try {
            var filePath = new Path(fileNamePath);
            var configuration = new Configuration();
            writer = OrcFile.createWriter(filePath,
                    OrcFile.writerOptions(configuration)
                            .setSchema(schema)
                            .overwrite(true)
            );
        } catch (IOException e) {
            throw new ORCFileException(e.getLocalizedMessage(), e);
        }
        return writer;
    }

    private String orcExceptionMsg(String prefixMsg, String fieldName, int rowNum) {
        return prefixMsg + fieldName + " in row " + rowNum;
    }

    /**
     * Add a column value that is a String or a byte[] array.
     *
     * @param colVal the column value object
     * @param fieldName the name of the field (for error reporting)
     * @param bytesColVector the BytesColumnVector that the byte array will be added to.
     * @param rowNum the ORC file row number
     */
    private void setByteColumnVector(Object colVal, String fieldName, BytesColumnVector bytesColVector, int rowNum) throws ORCFileException {
        if (colVal instanceof byte[] || colVal instanceof String) {
            byte[] byteVec;
            if (colVal instanceof String) {
                String strVal = (String)colVal;
                byteVec = strVal.getBytes(StandardCharsets.UTF_8);
            } else {
                byteVec = (byte[])colVal;
            }
            bytesColVector.setRef(rowNum, byteVec, 0, byteVec.length);
        } else {
            throw new ORCFileException(orcExceptionMsg("byte[] or String type expected for field ", fieldName, rowNum));
        }
    }

    private void setDecimalVector(Object colVal, String fieldName, DecimalColumnVector decimalColVector, int rowNum) throws ORCFileException {
        if (colVal instanceof BigDecimal) {
            var bigDecimal = (BigDecimal)colVal;
            decimalColVector.precision = (short)bigDecimal.precision();
            decimalColVector.scale = (short)bigDecimal.scale();
            HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
            var writeableDecimal = new HiveDecimalWritable(hiveDecimal);
            decimalColVector.vector[rowNum] = writeableDecimal;
        } else {
            throw new ORCFileException(orcExceptionMsg("BigDecimal type expected for field ",  fieldName, rowNum));
        }
    }


    private void setDoubleVector(Object colVal, String fieldName, DoubleColumnVector doubleVector, int rowNum) throws ORCFileException {
        if (colVal instanceof Double) {
            doubleVector.vector[rowNum] = (Double)colVal;
        } else if (colVal instanceof Float) {
            Float fltVal = (Float)colVal;
            doubleVector.vector[rowNum] = fltVal.doubleValue();
        } else {
            throw new ORCFileException(orcExceptionMsg("Double or Float type expected for field ",  fieldName, rowNum));
        }
    }

    /**
     * Initialize a LongColumnVector value.
     * @param colVal an object of type Boolean, Integer, Long or BigInteger.
     * @param fieldName the field name in the schema
     * @param longVector the LongColumnVector
     * @param rowNum the row number
     */
    private void setLongColumnVector(Object colVal, String fieldName, LongColumnVector longVector, int rowNum) throws ORCFileException {
        if (colVal instanceof Boolean) {
            Boolean bool = (Boolean)colVal;
            longVector.vector[rowNum] = (bool.equals(Boolean.TRUE)) ? Long.valueOf(1) : Long.valueOf(0);
        }  else if (colVal instanceof Integer) {
            longVector.vector[rowNum] = (Integer) colVal;
        } else if (colVal instanceof Long) {
            longVector.vector[rowNum] = (Long)colVal;
        } else if (colVal instanceof BigInteger) {
            BigInteger bigInt = (BigInteger) colVal;
            longVector.vector[rowNum] = bigInt.longValue();
        } else {
            throw new ORCFileException(orcExceptionMsg("Long or Integer type expected for field ",  fieldName, rowNum));
        }
    }

    private void setDateColumnVector(Object colVal, String fieldName, DateColumnVector dateVector, int rowNum) throws ORCFileException {
        if (colVal instanceof Date) {
            Date dateVal = (Date)colVal;
            long epochTime = dateVal.getTime();
            dateVector.vector[rowNum] = epochTime;
        } else {
            throw new ORCFileException(orcExceptionMsg("Date type expected for field ",  fieldName, rowNum));
        }
    }


    private void setTimestampVector(Object colVal, String fieldName, TimestampColumnVector timestampVector, int rowNum) throws ORCFileException {
        if (colVal instanceof Date) {
            var date = (Date)colVal;
            var ts = new Timestamp(date.getTime());
            timestampVector.set(rowNum, ts);
        } else {
            throw new ORCFileException(orcExceptionMsg("Date or Timestamp type expected for field ",  fieldName , rowNum));
        }
    }


    private void setStructColumnVector(Object colVal, TypeDescription typeDesc, String fieldName, StructColumnVector structVector, int rowNum)
            throws ORCFileException {
        if (colVal instanceof ArrayList) {
            @SuppressWarnings("unchecked")
            List<Object> fieldValList = (ArrayList<Object>)colVal;
            TypeDescription structSchema = schema.findSubtype(fieldName);
            ColumnVector[] structFieldVec = structVector.fields;
            List<String> structFieldNames = structSchema.getFieldNames();
            if (fieldValList.size() == structFieldNames.size()) {
                List<TypeDescription> structFieldTypes = typeDesc.getChildren();
                for (var ix = 0; ix < fieldValList.size(); ix++) {
                    setColumn(fieldValList.get(ix), structFieldTypes.get(ix), structFieldNames.get(ix), structFieldVec[ix], rowNum);
                }
            } else {
                throw new ORCFileException("Number of fields does not match the number of struct values");
            }
        } else {
            throw new ORCFileException(orcExceptionMsg("List<Object> type expected for field ", fieldName, rowNum));
        }
    }

    /**
     * <p>
     *     A union column can contain column vectors of more than one type. In the TypeDescription createUnion() is
     *     called to create a TypeDescription for a union column. The union values are added by calling
     *     the addUnionChild() method on this TypeDescription object.
     * </p>
     * <p>
     *     The class fields in the UnionColumnVector are shown below:
     * </p>
     * <pre>
     *     public class UnionColumnVector extends ColumnVector {
     *        public int[] tags;
     *        public ColumnVector[] fields;
     * </pre>
     * <p>
     *     A tag value (<pre>tags[rowNum]</pre>) is associated with each field value (<pre>fields[rowNum])</pre>.
     *     I have not seen an explicit definition for the tag value in the ORC documentation or in the Hive code
     *     for the UnionColumnVector. The code in this method assumes that the tag value is the
     *     enumeration value stored in a ColumnVector (<pre>ColumnVector.Type</pre>).
     *</p>
     * <p>
     *     The tag value is needed to initialize the ColumnVector since without the tag there is no way to know
     *     which union child should be initialized.
     * </p>
     *
     * @param colVal a Pair&lt;ColumnVector.Type, Object&gt; object with the union type and the object that will be used to
     *               initialize the union child ColumnVector.
     * @param fieldName The name of the union field
     * @param unionVector The UnionColumnVector to be initialized
     * @param rowNum the ORC file row number.
     */
    private void setUnionColumnVector(Object colVal, TypeDescription unionTypeDesc, String fieldName, UnionColumnVector unionVector, int rowNum) throws ORCFileException {
        @SuppressWarnings("unchecked")
        Pair<TypeDescription, Object> unionValuePair = (Pair<TypeDescription, Object>)colVal;
        TypeDescription unionValType = unionValuePair.getLeft();
        List<TypeDescription> unionChildrenTypes = unionTypeDesc.getChildren();
        Object unionColVal = unionValuePair.getRight();
        var found = false;
        for (var i = 0; i < unionChildrenTypes.size(); i++) {
            if (unionChildrenTypes.get(i).getCategory() == unionValType.getCategory()) {
                unionVector.tags[rowNum] = i;
                ColumnVector unionFieldVec = unionVector.fields[i];
                setColumn(unionColVal, unionChildrenTypes.get(i), fieldName, unionFieldVec, rowNum);
                found = true;
                break;
            }
        }
        if (! found) {
            throw new ORCFileException("writeUnionColumnVector: Bad type enumeration " + unionValType.getCategory().getName() + " passed for field " + fieldName );
        }
    }

    private void setLongListVector(List<Object> longValList, LongColumnVector longVector, int offset, String fieldName) throws ORCFileException {
        for (var i = 0; i < longValList.size(); i++) {
            Object objVal = longValList.get(i);
            if (objVal != null) {
                if (objVal instanceof Integer) {
                    longVector.vector[offset + i] = (Integer) objVal;
                } else if (objVal instanceof Long) {
                    longVector.vector[offset + i] = (Long) objVal;
                } else {
                    throw new ORCFileException("List<Integer> expected for field " + fieldName);
                }
            } else {
                longVector.isNull[offset + i] = true;
                longVector.noNulls = false;
            }
        }
    }

    private void setLongList(List<Object> colValList, ListColumnVector listVector, String fieldName, int rowNum) throws ORCFileException {
        LongColumnVector longVector = (LongColumnVector) listVector.child;
        int offset = (int) listVector.offsets[rowNum];
        setLongListVector(colValList, longVector, offset, fieldName);
    }

    private void setDoubleListVector(List<Object> doubleValList, DoubleColumnVector doubleVector, int offset, String fieldName) throws ORCFileException {
        for (var i = 0; i < doubleValList.size(); i++) {
            Object objVal = doubleValList.get(i);
            if (objVal != null) {
                if (objVal instanceof Double) {
                    doubleVector.vector[offset + i] = (Double) objVal;
                } else if (objVal instanceof Float) {
                    Float fltVal = (Float)objVal;
                    doubleVector.vector[offset + i] = fltVal.doubleValue();
                } else {
                    throw new ORCFileException("List<Double> expected for field " + fieldName);
                }
            } else {
                doubleVector.isNull[offset + i] = true;
                doubleVector.noNulls = false;
            }
        }
    }

    private void setDoubleList(List<Object> doubleValList, ListColumnVector listVector, String fieldName, int rowNum) throws ORCFileException {
        DoubleColumnVector vecChild = (DoubleColumnVector) listVector.child;
        int offset = (int) listVector.offsets[rowNum];
        setDoubleListVector(doubleValList, vecChild, offset, fieldName);
    }

    private void setTimestampListVector(List<Object> valueList, TimestampColumnVector timestampVector, int offset, String fieldName) throws ORCFileException {
        for (int i = 0; i < valueList.size(); i++) {
            Object objVal = valueList.get(i);
            if (objVal != null) {
                if (objVal instanceof Date) {
                    Timestamp ts = (objVal instanceof Timestamp) ? (Timestamp) objVal : new Timestamp(((Date)objVal).getTime());
                    timestampVector.time[offset + i] = ts.getTime();
                    timestampVector.nanos[offset + i] = ts.getNanos();
                } else {
                    throw new ORCFileException("List<Date> or List<Timestamp> expected for field " + fieldName);
                }
            } else {
                timestampVector.isNull[offset + i] = true;
                timestampVector.noNulls = false;
            }
        }
    }

    /**
     *  Initialize the vector values for a ListColumnVector of Date or Timestamp values.
     *
     * @param colValList a list of Timestamp or java.util.Date objects
     * @param listVector A ListColumnVector with a child that will contain the vector values.
     * @param fieldName The field name in the schema for this ORC element
     * @param rowNum The ORC file row number
     */
    private void setTimestampList(List<Object> colValList, ListColumnVector listVector, String fieldName, int rowNum) throws ORCFileException {
        TimestampColumnVector timestampVector = (TimestampColumnVector) listVector.child;
        int offset = (int) listVector.offsets[rowNum];
        setTimestampListVector(colValList, timestampVector, offset, fieldName);
    }

    private void setDecimalListVector(List<Object> decimalValList, DecimalColumnVector decimalVector, int offset, String fieldName) throws ORCFileException {
        for (var i = 0; i < decimalValList.size(); i++) {
            Object objVal = decimalValList.get(i);
            if (objVal != null) {
                if (objVal instanceof BigDecimal) {
                    var bigDecimal = (BigDecimal)objVal;
                    decimalVector.precision = (short)bigDecimal.precision();
                    decimalVector.scale = (short)bigDecimal.scale();
                    HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
                    var writeableDecimal = new HiveDecimalWritable(hiveDecimal);
                    decimalVector.vector[offset + i] = writeableDecimal;
                } else {
                    throw new ORCFileException("BigDecimal value expected for field " + fieldName);
                }
            } else {
                decimalVector.isNull[offset + i] = true;
                decimalVector.noNulls = false;
            }
        }
    }

    /**
     *
     * @param colValList a list of BigDecimal values to initialize the ListColumnVector child
     * @param listVector the ListColumnVector with the DecimalColumnVector child
     * @param fieldName the field name for the ListColumnVector/DecimalColumnVector column
     * @param rowNum the ORC file row number
     */
    private void setDecimalList(List<Object> colValList, ListColumnVector listVector, String fieldName, int rowNum) throws ORCFileException {
        DecimalColumnVector decimalVector = (DecimalColumnVector) listVector.child;
        int offset = (int) listVector.offsets[rowNum];
        setDecimalListVector(colValList, decimalVector, offset, fieldName);
    }

    private void setBytesListVector(List<Object> valueList, BytesColumnVector bytesVector, int offset, String fieldName) throws ORCFileException {
        for (var i = 0; i < valueList.size(); i++) {
            Object objVal = valueList.get(i);
            if (objVal != null) {
                if (objVal instanceof byte[] || objVal instanceof String) {
                    byte[] byteVec = (objVal instanceof byte[]) ? (byte[])objVal : ((String)objVal).getBytes(StandardCharsets.UTF_8);
                    bytesVector.vector[offset + i] = byteVec;
                    bytesVector.length[offset + i] = byteVec.length;
                } else {
                    throw new ORCFileException("String or byte[] value expected for field " + fieldName);
                }
            } else {
                bytesVector.isNull[offset + i] = true;
                bytesVector.length[offset + i] = 0;
                bytesVector.noNulls = false;
            }
        }
    }

    /**
     * Initialize a ListColumnVector with a BytesColumnVector child with byte[] values.
     *
     * @param colValList a list of byte[] or String values
     * @param listVector the parent ListColumnVector
     * @param fieldName the field name for the ORC column that contains the ListColumnVector
     * @param rowNum the ORC file row number
     */
    private void setBytesList(List<Object> colValList, ListColumnVector listVector, String fieldName, int rowNum) throws ORCFileException {
        BytesColumnVector bytesVector = (BytesColumnVector) listVector.child;
        int offset = (int) listVector.offsets[rowNum];
        setBytesListVector(colValList, bytesVector, offset, fieldName);
    }


    private void setMultiValuedVectorParameters(MultiValuedColumnVector multiVector, int vecLength, int rowNum) {
        multiVector.lengths[rowNum] = vecLength;
        if (rowNum > 0) {
            multiVector.offsets[rowNum] = multiVector.lengths[rowNum - 1] + multiVector.offsets[rowNum - 1];
        }
    }


    private void setListVectorParameters(ListColumnVector listVec, int maxBatchSize, int vecLength, int rowNum ) {
        setMultiValuedVectorParameters(listVec, vecLength, rowNum);
        listVec.child.ensureSize(maxBatchSize * vecLength, true);
    }

    /**
     * Initialize a ListColumnVector. The child of the vector is limited to the scalar types long, double, String (or byte[])), BigDecimal or
     * Date (or Timestamp).
     *
     * @param colVal a List&lt;Object&gt;
     * @param typeDesc the schema definition for this column
     * @param fieldName the column field name
     * @param listVector the ListColumnVector parent of the vector type child
     * @param rowNum the ORC file row number.
     */
    private void setListColumnVector(Object colVal, TypeDescription typeDesc, String fieldName, ListColumnVector listVector, int rowNum) throws ORCFileException {
        if (colVal instanceof ArrayList) {
            @SuppressWarnings("unchecked")
            List<Object> objValList = (ArrayList<Object>) colVal;
            final int maxBatchSize = typeDesc.createRowBatch().getMaxSize();
            setListVectorParameters(listVector, maxBatchSize, objValList.size(), rowNum);
            ColumnVector.Type childType = listVector.child.type;
            switch (childType) {
                case LONG -> setLongList(objValList, listVector, fieldName, rowNum);
                case DOUBLE -> setDoubleList(objValList, listVector, fieldName, rowNum);
                case BYTES -> setBytesList(objValList, listVector, fieldName, rowNum);
                case DECIMAL -> setDecimalList(objValList, listVector, fieldName, rowNum);
                case TIMESTAMP -> setTimestampList(objValList, listVector, fieldName, rowNum);
                default -> throw new ORCFileException(childType.name() + " is not supported for ListColumnVector columns");
            }
        } else {
            throw new ORCFileException("ArrayList value expected for field " + fieldName);
        }
    }

    /**
     * Test that all elements in an Object list are of a particular type
     * @param objList the Object list that is tested
     * @param typeTest a function that compares against a particular Object type
     * @return true if all elements are of the test type, false if one or more elements are not of that type.
     */
    private boolean isListType(List<Object> objList, Predicate<Object> typeTest) {
        return ! objList.stream().map(typeTest::test).collect(Collectors.toList()).contains(false);
    }

    /**
     * Initialize a ColumnVector with Long values.
     *
     * @param valueList a list of Long values
     * @param colVector the LongColumnVector that will be initialized with the Long values
     * @param offset the offset[rownum] value for the array
     * @param fieldName the field name for the Map column
     */
    private void setLongMapValues(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        if (isListType(valueList, isLong) || isListType(valueList, isInteger)) {
            LongColumnVector longVector = (LongColumnVector) colVector;
            setLongListVector(valueList, longVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " Long values expected");
        }
    }

    /**
     * Initialize a ColumnVector with Double values.
     *
     * @param valueList a list of Double values
     * @param colVector the DoubleColumnVector that will be initialized with the Double values
     * @param offset the offset[rownum] value for the array
     * @param fieldName the field name for the Map column
     */
    private void setDoubleMapValues(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        if (isListType(valueList, isDouble)) {
            DoubleColumnVector doubleVector = (DoubleColumnVector) colVector;
            setDoubleListVector(valueList, doubleVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " Double values expected");
        }
    }

    /**
     * Initialize a ColumnVector with String values.
     *
     * @param valueList a list of String values
     * @param colVector the BytesColumnVector that will be initialized with the String values
     * @param offset the offset[rownum] value for the array
     * @param fieldName the field name for the Map column
     */
    private void setStringMapValues(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        if (isListType(valueList, isString)) {
            BytesColumnVector doubleVector = (BytesColumnVector) colVector;
            setBytesListVector(valueList, doubleVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " String values expected");
        }
    }

    /**
     * Initialize a ColumnVector with BigDeciml values.
     *
     * @param valueList a list of BigDecimal
     * @param colVector the DecimalColumnVector that will be initialized with the BigDecimal values
     * @param offset the offset[rownum] value for the array
     * @param fieldName the field name for the Map column
     */
    private void setDecimalMapValues(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        if (isListType(valueList, isBigDecimal)) {
            DecimalColumnVector decimalVector = (DecimalColumnVector) colVector;
            setDecimalListVector(valueList, decimalVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " BigDecimal values expected");
        }
    }

    /**
     * Initialize a ColumnVector with timestamp values.
     *
     * @param valueList a list of Date (or Timestamp) objects
     * @param colVector the TimestampColumnVector that will be initialized with the Timestamp values
     * @param offset the offset[rownum] value for the array
     * @param fieldName the field name for the Map column
     */
    private void setTimestampMapValues(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        if (isListType(valueList, isDate)) {
            TimestampColumnVector timestampVector = (TimestampColumnVector) colVector;
            setTimestampListVector(valueList, timestampVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " Date or Timestamp values expected");
        }
    }


    /**
     * Set the MapColumn value array vector. The type for this vector is limited to long, double, bytes (String),
     * Decimal and Timestamp.
     *
     * @param valueList a list of Objects to initialize the Map column value array.
     * @param colVector the column array vector to be initialized with the map values.
     * @param offset the offset[rowNum] from the parent MapColumnVector
     * @param fieldName the name of the field for the MapColumnVector.
     */
    private void setMapValueVector(List<Object> valueList, ColumnVector colVector, int offset, String fieldName) throws ORCFileException {
        switch (colVector.type) {
            case LONG -> setLongMapValues(valueList, colVector, offset, fieldName);
            case DOUBLE -> setDoubleMapValues(valueList, colVector, offset, fieldName);
            case BYTES -> setStringMapValues(valueList, colVector, offset, fieldName);
            case DECIMAL -> setDecimalMapValues(valueList, colVector, offset, fieldName);
            case TIMESTAMP -> setTimestampMapValues(valueList, colVector, offset, fieldName);
            default -> throw new ORCFileException("For field " + fieldName + " values must be long, double, String, BigDecimal or Date/Timestamp");
        }
    }

    /**
     * <p>
     *     Initialize a MapColumnVector with Long key values.
     * </p>
     * @param mapSet a set of {key, value} pairs, where the key values are Long objects. The elements of this
     *               set will be used to initialize the key and value array column vectors that are children of the
     *               MapColumnVector.
     * @param mapVector the MapColumnVector. This ColumnVector has children for the key and value arrays.
     * @param fieldName the field name for the map column vector column.
     * @param rowNum the ORC file row number.
     */
    private void setLongKeyMap(Set<Map.Entry<Object, Object>> mapSet, MapColumnVector mapVector, String fieldName, int rowNum) throws ORCFileException {
        List<Object> keyValueList = mapSet.stream().map(Map.Entry::getKey).collect(Collectors.toList());
         if (isListType(keyValueList, isLong)) {
             LongColumnVector longVector = (LongColumnVector) mapVector.keys;
             int offset = (int)mapVector.offsets[rowNum];
             // set the key vector
             setLongListVector(keyValueList, longVector, offset, fieldName);
             // set the value vector
             ColumnVector valueVector = mapVector.values;
             List<Object> valueList = mapSet.stream().map(Map.Entry::getValue).collect(Collectors.toList());
             setMapValueVector(valueList, valueVector, offset, fieldName);
         } else {
             throw new ORCFileException("For field " + fieldName + " Long key type expected to match schema");
         }
    }

    /**
     * <p>
     *     Initialize a MapColumnVector with Double key values.
     * </p>
     * @param mapSet a set of {key, value} pairs, where the key values are Double objects. The elements of this
     *               set will be used to initialize the key and value array column vectors that are children of the
     *               MapColumnVector.
     * @param mapVector the MapColumnVector. This ColumnVector has children for the key and value arrays.
     * @param fieldName the field name for the map column vector column.
     * @param rowNum the ORC file row number.
     */
    private void setDoubleKeyMap(Set<Map.Entry<Object, Object>> mapSet, MapColumnVector mapVector, String fieldName, int rowNum) throws ORCFileException {
        List<Object> keyValueList = mapSet.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        if (isListType(keyValueList, isDouble)) {
            DoubleColumnVector doubleVector = (DoubleColumnVector) mapVector.keys;
            int offset = (int)mapVector.offsets[rowNum];
            // set the key vector
            setDoubleListVector(keyValueList, doubleVector, offset, fieldName);
            // set the value vector
            ColumnVector valueVector = mapVector.values;
            List<Object> valueList = mapSet.stream().map(Map.Entry::getValue).collect(Collectors.toList());
            setMapValueVector(valueList, valueVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " Long key type expected to match schema");
        }
    }


    /**
     * <p>
     *     Initialize a MapColumnVector with String key values.
     * </p>
     * @param mapSet a set of {key, value} pairs, where the key values are String objects. The elements of this
     *               set will be used to initialize the key and value array column vectors that are children of the
     *               MapColumnVector.
     * @param mapVector the MapColumnVector. This ColumnVector has children for the key and value arrays.
     * @param fieldName the field name for the map column vector column.
     * @param rowNum the ORC file row number.
     */
    private void setStringKeyMap(Set<Map.Entry<Object, Object>> mapSet, MapColumnVector mapVector, String fieldName, int rowNum) throws ORCFileException {
        List<Object> keyValueList = mapSet.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        if (isListType(keyValueList, isString)) {
            BytesColumnVector byteVector = (BytesColumnVector) mapVector.keys;
            int offset = (int)mapVector.offsets[rowNum];
            // set the key array vector
            setBytesListVector(keyValueList, byteVector, offset, fieldName);
            // set the value array vector
            ColumnVector valueVector = mapVector.values;
            List<Object> valueList = mapSet.stream().map(Map.Entry::getValue).collect(Collectors.toList());
            setMapValueVector(valueList, valueVector, offset, fieldName);
        } else {
            throw new ORCFileException("For field " + fieldName + " Long key type expected to match schema");
        }
    }

    private void setMapVectorParameters(MapColumnVector mapVec, int maxBatchSize, int vecLength, int rowNum) {
        setMultiValuedVectorParameters(mapVec, vecLength, rowNum);
        mapVec.keys.ensureSize(maxBatchSize + vecLength, true);
        mapVec.values.ensureSize(maxBatchSize + vecLength, true);
    }


    /**
     * <p>
     *     Set the Map key and value elements for a MapColumnVector
     * </p>
     * <p>
     * A MapColumnVector has a single ColumnVector type for each of the map key and map values. For example, the ColumnVector
     * for the key values could be a BytesColumnVector (a set of String keys). The values could be a LongColumnVector.
     * </p>
     * <p>
     * In the documentation there is no restriction given for
     * the map key type. This code limits the key types to scalar values: string, long, double.
     * </p>
     * </p>
     * <p>
     *     The documentation does not limit the map value types. This code limites the map values
     *     to the same types that are supported for ListColumnVectors: long, double, bytes (String),
     *     Decimal and Timestamp.
     * </p>
     *
     * @param colVal    a HashMap object
     * @param typeDesc  the schema description for the MapColumnVector column
     * @param fieldName the field name of the MapColumnVector column
     * @param mapVector The parent MapColumnVector
     * @param rowNum    the ORC file column number.
     */
    private void setMapColumnVector(Object colVal, TypeDescription typeDesc, String fieldName, MapColumnVector mapVector, int rowNum) throws ORCFileException {
        if (colVal == null) {
            mapVector.isNull[rowNum] = true;
            mapVector.noNulls = false;
        } else {
            if (colVal instanceof HashMap) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> rawMap = (HashMap<Object, Object>)colVal;
                int mapLen = rawMap.size();
                final int maxBatchSize = typeDesc.createRowBatch().getMaxSize();
                setMapVectorParameters(mapVector, maxBatchSize, mapLen, rowNum);
                if (checkMapColumnVectorTypes(mapVector)) {
                    Set<Map.Entry<Object, Object>> mapSet = rawMap.entrySet();
                    switch (mapVector.keys.type) {
                        case LONG -> setLongKeyMap(mapSet, mapVector, fieldName, rowNum);
                        case DOUBLE ->  setDoubleKeyMap(mapSet, mapVector, fieldName, rowNum);
                        case BYTES -> setStringKeyMap(mapSet, mapVector, fieldName, rowNum);
                        default -> { /* This block left intentionally empty */ }
                    }
                } else {
                    throw new ORCFileException("For field " + fieldName + " key types are limited to string, long and double. " +
                            "value types are limited to long, double, String, decimal and timestamp");
                }
            }
        }
    }


    /**
     * Set a column value in an ORC a row that will be written to the ORC file.
     *
     * @param colVal an Object containing the values to be written to the column
     * @param typeDesc the TypeDescription from the schema that defines the column
     * @param fieldName the column field name
     * @param vector the ColumnVector that will be initialized with the values in the colVal argument.
     * @param rowNum the ORC file row number.
     */
    private void setColumn(Object colVal, TypeDescription typeDesc, String fieldName, ColumnVector vector, int rowNum) throws ORCFileException {
        if (colVal == null) {
            vector.isNull[rowNum] = true;
            vector.noNulls = false;
        } else {
            switch (vector.type) {
                case LONG -> {
                    if (vector instanceof DateColumnVector) {
                        // When a DateColumnVector epoch time value is written to the ORC file, it is incorrectly
                        // stored as a 32 bit int, instead of a 64 bit long. When this value is read from the
                        // ORC file, the epoch value is incorrect.  Until this error is fixed in the ORC Writer
                        // the DateColumnVector is not supported.
                        throw new ORCFileException("The date column type is not supported. Please use the timestamp type for field " + fieldName);
                    } else {
                        LongColumnVector longVector = (LongColumnVector) vector;
                        setLongColumnVector(colVal, fieldName, longVector, rowNum);
                    }
                }
                case DOUBLE -> {
                    DoubleColumnVector doubleVector = (DoubleColumnVector)vector;
                    setDoubleVector(colVal, fieldName, doubleVector, rowNum);
                }
                case BYTES -> {
                    BytesColumnVector bytesColVector = (BytesColumnVector) vector;
                    setByteColumnVector(colVal, fieldName, bytesColVector, rowNum);
                }
                case DECIMAL -> {
                    DecimalColumnVector decimalVector = (DecimalColumnVector) vector;
                    setDecimalVector(colVal, fieldName, decimalVector, rowNum);
                }
                case DECIMAL_64 -> throw new ORCFileException("Field: " + fieldName + ", Decimal64ColumnVector is not supported");
                case TIMESTAMP -> {
                    TimestampColumnVector timestampVector = (TimestampColumnVector) vector;
                    setTimestampVector(colVal, fieldName, timestampVector, rowNum);
                }
                case INTERVAL_DAY_TIME -> throw new ORCFileException("Field: " + fieldName + ", HiveIntervalDayTime is not supported");
                case STRUCT -> {
                    StructColumnVector structVector = (StructColumnVector) vector;
                    setStructColumnVector(colVal, typeDesc, fieldName, structVector, rowNum);
                }
                case LIST -> {
                    ListColumnVector listVector = (ListColumnVector) vector;
                    setListColumnVector(colVal, typeDesc, fieldName, listVector, rowNum);
                }
                case MAP -> {
                    MapColumnVector mapVector = (MapColumnVector) vector;
                    setMapColumnVector(colVal, typeDesc, fieldName, mapVector, rowNum);
                }
                case UNION -> {
                    UnionColumnVector unionVector = (UnionColumnVector) vector;
                    setUnionColumnVector(colVal, typeDesc, fieldName, unionVector, rowNum);
                }
                default -> throw new ORCFileException("setColumn: Internal error: unexpected ColumnVector subtype");
            } // switch
        } // else
    } // setColumn


    /**
     * <p>
     * Write an ORC file row
     * </p>
     *
     * @param row Each element in the List corresponds to a column in the ORC file as defined by the ORC
     *            file schema. The Objects in the list may be complex object like Lists in the case of
     *            columns that consists of array, structure or map elements.
     */
    public void writeRow(List<Object> row) throws ORCFileException {
        if (orcWriter == null) {
            orcWriter = buildOrcWriter();
        }
        List<TypeDescription> children = schema.getChildren();
        for (var colIx = 0; colIx < row.size(); colIx++) {
            setColumn(row.get(colIx), children.get(colIx), fieldNames.get(colIx), batch.cols[colIx], batch.size);
        }
        batch.size++;
        try {
            if (batch.size == batch.getMaxSize()) {
                orcWriter.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            throw new ORCFileException("IOException writing ORC batch: " + e.getLocalizedMessage());
        }
    } // writeRow

    /**
     * <p>
     * Close method for the WriteORCFile object. Once this function is called the object should not be reused.
     * </p>
     * <p>
     *     This class is designed to be used inside a try() { .. } operation. For example:
     * </p>
     * <pre>
     *      try (var orcWriter = new WriteORCFile(filePath, schema)) {
     *         ...
     *      }
     * </pre>
     */
    @Override
    public void close() throws ORCFileException {
        if (orcWriter != null) {
            try {
                if (batch.size > 0) {
                    orcWriter.addRowBatch(batch);
                }
                orcWriter.close();
            } catch (IOException e) {
                throw new ORCFileException(e.getLocalizedMessage(), e);
            }
        }
    }

}
