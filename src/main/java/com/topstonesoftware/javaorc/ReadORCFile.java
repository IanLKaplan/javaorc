/*
 * Ian L. Kaplan (iank@bearcave.com) and Topstone Software Consulting
 * licenses this software to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.topstonesoftware.javaorc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**
 * <p>
 * An ORC file reader
 *</p>
 * <p>
 *     The ORC file reader is a reader for one file and cannot be reused.
 * </p>
 * <p>
 *     This reader is not thread safe.
 * </p>
 *
 * <h4>References</h4>
 *
 *  <h4>
 *     API
 *  </h4>
 *  <ul>
 *     <li>Apache ORC types: https://orc.apache.org/docs/types.html</li>
 *     <li>Apache ORC documentation: https://orc.apache.org/docs/core-java.html</li>
 *     <li>ORC Core: https://javadoc.io/doc/org.apache.orc/orc-core/latest/index.html</li>
 *     <li>HIVE Storage API: https://orc.apache.org/api/hive-storage-api/index.html</li>
 *  </ul>
 */
public class ReadORCFile extends ORCFileIO implements AutoCloseable {
    // Supported types for ListColumnVectors
    private final String orcFilePath;
    private Reader orcFileReader = null;
    private RecordReader recordReader = null;
    private VectorizedRowBatch batch = null;
    private TypeDescription schema = null;
    private int batchRowNum = 0;
    private int fileRowNum = 0;
    private boolean readBatch = false;

    ReadORCFile(String orcFilePath) {
        this.orcFilePath = orcFilePath;
    }

    void setOrcFileReader(Reader orcReader) {
        orcFileReader = orcReader;
    }

    private Reader buildOrcReader() throws ORCFileException {
        Reader reader;
        try {
            var filePath = new Path(this.orcFilePath);
            var configuration = new Configuration();
            reader = OrcFile.createReader(filePath,
                    OrcFile.readerOptions(configuration));
        } catch (IOException e) {
            throw new ORCFileException(e.getLocalizedMessage(), e);
        }
        return reader;
    }

    private void allocateReader() throws ORCFileException {
        if (orcFileReader == null) {
            orcFileReader = buildOrcReader();
            try {
                recordReader = orcFileReader.rows();
            } catch (IOException e) {
                throw new ORCFileException( e.getLocalizedMessage() );
            }
        }
    }

    private void allocateBatch() throws ORCFileException {
        if (batch == null) {
            allocateReader();
            batch = getSchema().createRowBatch();
        }
    }

    TypeDescription getSchema() throws ORCFileException {
        allocateReader();
        if (schema == null) {
            schema = orcFileReader.getSchema();
        }
        return schema;
    }

    /**
     * <p>
     * Read a Union column value
     * </p>
     * The UnionColumnVector class is outlined below:
     * <pre>
     *    public class UnionColumnVector extends ColumnVector {
     *       public int[] tags;
     *       public ColumnVector[] fields;
     *       ...
     * </pre>
     * <p>
     *     The tags[rowNum] value corresponds to fields[rowNum]
     * </p>
     * <P>
     *     The function returns a Pair&lt;ColumnVector.Type, Object&gt; where the Object is the column value.
     *     The column value may be any value that is supported by the readColumn() function. This allows
     *     recursively complex data structures, which may or may not be a good idea.
     * </P>
     */
    private Object readUnionVal(ColumnVector colVec, TypeDescription colType, int rowNum) throws ORCFileException {
        Pair<TypeDescription, Object> columnValuePair;
        UnionColumnVector unionVector = (UnionColumnVector) colVec;
        int tagVal = unionVector.tags[rowNum];
        List<TypeDescription> unionFieldTypes = colType.getChildren();
        if (tagVal < unionFieldTypes.size()) {
            TypeDescription fieldType = unionFieldTypes.get(tagVal);
            if (tagVal < unionVector.fields.length) {
                ColumnVector fieldVector = unionVector.fields[tagVal];
                Object unionValue = readColumn(fieldVector, fieldType, rowNum);
                columnValuePair = new ImmutablePair<>(fieldType, unionValue);
            } else {
                throw new ORCFileException("readUnionVal: union tag value out of range for union column vectors");
            }
        } else {
            throw new ORCFileException("readUnionVal: union tag value out of range for union types");
        }
        return columnValuePair;
    }


    private List<Object> readMapVector(ColumnVector mapVector, TypeDescription childType, int offset, int numValues) throws ORCFileException {
        @SuppressWarnings("unchecked")
        List<Object> mapList = (List<Object>)switch (mapVector.type) {
            case BYTES -> readBytesListVector((BytesColumnVector) mapVector, childType, offset, numValues);
            case LONG -> readLongListVector((LongColumnVector)mapVector, childType, offset, numValues);
            case DOUBLE -> readDoubleListVector((DoubleColumnVector) mapVector, offset, numValues);
            case DECIMAL -> readDecimalListVector((DecimalColumnVector) mapVector, offset, numValues);
            case TIMESTAMP -> readTimestampListVector((TimestampColumnVector)mapVector, childType, offset, numValues);
            default -> throw new ORCFileException(mapVector.type.name() + " is not supported for MapColumnVectors");
        };
        return mapList;
    }

    /**
     * <p>
     * Read a Map column value (e.g., a set of keys and their associated values).
     * </p>
     * <p>
     * The Map key and value types are the first and second children in the children TypeDescription List.
     * From the TypeDescription source:
     * </p>
     * <pre>
     *     result.children.add(keyType);
     *     result.children.add(valueType);
     * </pre>
     */
    private Object readMapVal(ColumnVector colVec, TypeDescription colType, int rowNum) throws ORCFileException {
        Map<Object, Object> objMap = new HashMap<>();
        MapColumnVector mapVector = (MapColumnVector) colVec;
        if (checkMapColumnVectorTypes(mapVector)) {
            int mapSize = (int)mapVector.lengths[ rowNum ];
            int offset = (int)mapVector.offsets[ rowNum ];
            List<TypeDescription> mapTypes = colType.getChildren();
            TypeDescription keyType = mapTypes.get(0);
            TypeDescription valueType = mapTypes.get(1);
            ColumnVector keyChild = mapVector.keys;
            ColumnVector valueChild = mapVector.values;
            List<Object> keyList = readMapVector(keyChild, keyType, offset, mapSize);
            List<Object> valueList = readMapVector(valueChild, valueType, offset, mapSize);
            assert(keyList.size() == valueList.size());
            for (var i = 0; i < keyList.size(); i++) {
                objMap.put(keyList.get(i), valueList.get(i));
            }
        } else {
            throw new ORCFileException("readMapVal: unsupported key or value types");
        }
        return objMap;
    }

    private List<Object> readLongListVector(LongColumnVector longVector, TypeDescription childType, int offset, int numValues) {
        List<Object> longList = new ArrayList<>();
        for (var i = 0; i < numValues; i++) {
            if (! longVector.isNull[offset + i]) {
                long longVal = longVector.vector[offset + i];
                if (childType.getCategory() == TypeDescription.Category.BOOLEAN) {
                    Boolean boolVal = longVal == 0 ? Boolean.valueOf(false) : Boolean.valueOf(true);
                    longList.add(boolVal);
                } else if (childType.getCategory() == TypeDescription.Category.INT) {
                    Integer intObj = (int) longVal;
                    longList.add( intObj );
                } else {
                    longList.add(longVal);
                }
            } else {
                longList.add( null );
            }
        }
        return longList;
    }

    private Object readLongListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int)listVector.offsets[rowNum];
        int numValues = (int)listVector.lengths[ rowNum ];
        LongColumnVector longVector = (LongColumnVector) listVector.child;
        return readLongListVector(longVector, childType, offset, numValues);
    }

    private Object readTimestampListVector(TimestampColumnVector timestampVector, TypeDescription childType, int offset, int numValues) {
        List<Object> timestampList = new ArrayList<>();
        for (var i = 0; i < numValues; i++) {
            if (! timestampVector.isNull[offset + i]) {
                int nanos = timestampVector.nanos[offset + i];
                long millisec = timestampVector.time[offset + i];
                Timestamp timestamp = new Timestamp(millisec);
                timestamp.setNanos(nanos);
                if (childType.getCategory() == TypeDescription.Category.DATE) {
                    Date date = new Date(timestamp.getTime());
                    timestampList.add(date);
                } else {
                    timestampList.add(timestamp);
                }
            } else {
                timestampList.add( null );
            }
        }
        return timestampList;
    }

    /**
     *  Read either Timestamp or Date values, depending on the definition in the schema.
     */
    private Object readTimestampListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int)listVector.offsets[rowNum];
        int numValues = (int)listVector.lengths[ rowNum ];
        TimestampColumnVector timestampVec = (TimestampColumnVector) listVector.child;
        return readTimestampListVector(timestampVec, childType, offset, numValues);
    }

    private Object readDecimalListVector(DecimalColumnVector decimalVector, int offset, int numValues) {
        List<Object> decimalList = new ArrayList<>();
        for (var i = 0; i < numValues; i++) {
            if (! decimalVector.isNull[ offset + i]) {
                BigDecimal bigDecimal = decimalVector.vector[batchRowNum].getHiveDecimal().bigDecimalValue();
                decimalList.add( bigDecimal );
            } else {
                decimalList.add(null);
            }
        }
        return decimalList;
    }

    private Object readDecimalListValues(ListColumnVector listVector, int rowNum) {
        int offset = (int)listVector.offsets[rowNum];
        int numValues = (int)listVector.lengths[ rowNum ];
        DecimalColumnVector decimalVec = (DecimalColumnVector) listVector.child;
        return readDecimalListVector(decimalVec, offset, numValues);
    }

    private Object readBytesListVector(BytesColumnVector bytesVec, TypeDescription childType, int offset, int numValues) {
        List<Object> bytesValList = new ArrayList<>();
        for (var i = 0; i < numValues; i++) {
            if (!bytesVec.isNull[offset + i]) {
                byte[] byteArray = bytesVec.vector[offset + i];
                int vecLen = bytesVec.length[offset + i];
                int vecStart = bytesVec.start[offset + i];
                byte[] vecCopy = Arrays.copyOfRange(byteArray, vecStart, vecStart + vecLen);
                if (childType.getCategory() == TypeDescription.Category.STRING) {
                    String str = new String( vecCopy );
                    bytesValList.add( str );
                } else {
                    bytesValList.add( vecCopy );
                }
            } else {
                bytesValList.add(null);
            }
        }
        return bytesValList;
    }

    private Object readBytesListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int)listVector.offsets[rowNum];
        int numValues = (int)listVector.lengths[ rowNum ];
        BytesColumnVector bytesVec = (BytesColumnVector) listVector.child;
        return readBytesListVector(bytesVec, childType, offset, numValues);
    }

    private Object readDoubleListVector(DoubleColumnVector doubleVec, int offset, int numValues) {
        List<Object> doubleList = new ArrayList<>();
        for (var i = 0; i < numValues; i++) {
            if (! doubleVec.isNull[offset + i]) {
                Double doubleVal = doubleVec.vector[offset + i];
                doubleList.add(doubleVal);
            } else {
                doubleList.add( null );
            }
        }
        return doubleList;
    }

    private Object readDoubleListValues(ListColumnVector listVector, int rowNum) {

        int offset = (int)listVector.offsets[rowNum];
        int numValues = (int)listVector.lengths[ rowNum ];
        DoubleColumnVector doubleVec = (DoubleColumnVector) listVector.child;
        return readDoubleListVector(doubleVec, offset, numValues);
    }

    /**
     *  Read a List column value. The List types are limited to Long, Double, Bytes, Decimal, Timestamp
     */
    private Object readListVal(ColumnVector colVec, TypeDescription colType, int rowNum) throws ORCFileException {
        Object listValues = null;
        if (! colVec.isNull[ rowNum ]) {
            ListColumnVector listVector = (ListColumnVector) colVec;
            ColumnVector listChildVector = listVector.child;
            TypeDescription childType = colType.getChildren().get(0);
            listValues = switch (listChildVector.type) {
                case LONG -> readLongListValues(listVector, childType, rowNum);
                case DOUBLE -> readDoubleListValues(listVector, rowNum);
                case BYTES -> readBytesListValues(listVector, childType, rowNum);
                case DECIMAL -> readDecimalListValues(listVector, rowNum);
                case TIMESTAMP -> readTimestampListValues(listVector, childType, rowNum);
                default -> throw new ORCFileException( listVector.type.name() + " is not supported for ListColumnVectors");
            };
        }
        return listValues;
    }


    private Object readStructVal(ColumnVector colVec, TypeDescription colType, int rowNum) throws ORCFileException {
        Object structObj = null;
        if (! colVec.isNull[ rowNum ]) {
            List<Object> fieldValList = new ArrayList<>();
            StructColumnVector structVector = (StructColumnVector) colVec;
            ColumnVector[] fieldVec = structVector.fields;
            List<TypeDescription> fieldTypes = colType.getChildren();
            assert(fieldVec.length == fieldTypes.size());
            for (var i = 0; i < fieldVec.length; i++) {
                Object fieldObj = readColumn(fieldVec[i], fieldTypes.get(i), rowNum);
                fieldValList.add(fieldObj);
            }
            structObj = fieldValList;
        }
        return structObj;
    }


    private Object readTimestampVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object timestampVal = null;
        if (! colVec.isNull[rowNum]) {
            TimestampColumnVector timestampVec = (TimestampColumnVector) colVec;
            int nanos = timestampVec.nanos[ rowNum ];
            long millisec = timestampVec.time[ rowNum ];
            Timestamp timestamp = new Timestamp( millisec );
            timestamp.setNanos(nanos);
            timestampVal = timestamp;
            if (colType.getCategory() == TypeDescription.Category.DATE) {
                timestampVal = new Date( timestamp.getTime());
            }
        }
        return timestampVal;
    }

    private Object readDecimalVal(ColumnVector colVec, int rowNum) {
        Object decimalObj = null;
        if (! colVec.isNull[rowNum]) {
            DecimalColumnVector decimalVec = (DecimalColumnVector) colVec;
            decimalObj = decimalVec.vector[rowNum].getHiveDecimal().bigDecimalValue();
        }
        return decimalObj;
    }

    /**
     * Read a Long or Boolean value
     * @param colVec the column vector
     * @param colType the type of the column
     * @param rowNum the ORC file row number.
     * @return a Boolean or Long object
     */
    private Object readLongVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object colObj = null;
        if (! colVec.isNull[rowNum]) {
            LongColumnVector longVec = (LongColumnVector) colVec;
            long longVal = longVec.vector[rowNum];
            colObj = longVal;
            if (colType.getCategory() == TypeDescription.Category.INT) {
                colObj = (int) longVal;
            } else if (colType.getCategory() == TypeDescription.Category.BOOLEAN) {
                colObj = longVal == 1 ? Boolean.TRUE : Boolean.FALSE;
            }
        }
        return colObj;
    }

    private Object readBytesVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object bytesObj = null;
        if (! colVec.isNull[rowNum]) {
            BytesColumnVector bytesVector = (BytesColumnVector) colVec;
            byte[] columnBytes = bytesVector.vector[rowNum];
            int vecLen = bytesVector.length[rowNum];
            int vecStart = bytesVector.start[rowNum];
            byte[] vecCopy = Arrays.copyOfRange(columnBytes, vecStart, vecStart + vecLen);
            if (colType.getCategory() == TypeDescription.Category.STRING) {
                bytesObj = new String(vecCopy);
            } else {
                bytesObj = vecCopy;
            }
        }
        return bytesObj;
    }


    private Object readColumn(ColumnVector colVec, TypeDescription colType, int rowNum) throws ORCFileException {
        Object columnObj = null;
        if (! colVec.isNull[rowNum]) {
            columnObj = switch (colVec.type) {
                case LONG -> readLongVal(colVec, colType, rowNum);
                case DOUBLE -> colVec.isNull[rowNum] ? null : ((DoubleColumnVector) colVec).vector[rowNum];
                case BYTES -> readBytesVal(colVec, colType, rowNum);
                case DECIMAL -> readDecimalVal(colVec, rowNum);
                case TIMESTAMP -> readTimestampVal(colVec, colType, rowNum);
                case STRUCT -> readStructVal(colVec, colType, rowNum);
                case LIST -> readListVal(colVec, colType, rowNum);
                case MAP -> readMapVal(colVec, colType, rowNum);
                case UNION -> readUnionVal(colVec, colType, rowNum);
                default -> throw new ORCFileException("readColumn: unsupported ORC file column type: " + colVec.type.name());
            };
        }
        return columnObj;
    }


    private void readBatchRow(int rowNum, List<Object> row) throws ORCFileException {
        final int numCols = batch.numCols;
        final ColumnVector[] cols = batch.cols;
        List<TypeDescription> colTypes = schema.getChildren();
        for (var colNum = 0; colNum < numCols; colNum++) {
            Object colObj = readColumn(cols[colNum], colTypes.get(colNum), rowNum);
            row.add(colObj);
        }
    }

    long getNumberOfRows() throws ORCFileException {
        allocateReader();
        return orcFileReader.getNumberOfRows();
    }


    List<Object> readRow() throws ORCFileException {
        allocateBatch();
        List<Object> row = new ArrayList<>();
        try {
            if (!readBatch) {
                recordReader.nextBatch(batch);
                readBatch = true;
            }
            if (batchRowNum == batch.getMaxSize()) {
                recordReader.nextBatch(batch);
                batchRowNum = 0;
            }
            if (fileRowNum < orcFileReader.getNumberOfRows()) {
                readBatchRow(batchRowNum, row);
                batchRowNum++;
                fileRowNum++;
            }
        } catch (IOException e) {
            throw new ORCFileException(e.getLocalizedMessage());
        }
        return row;
    }



    @Override
    public void close() throws ORCFileException {
        try {
            if (orcFileReader != null) {
                orcFileReader.close();
            }
            if (recordReader != null) {
                recordReader.close();
            }
        } catch (Exception e) {
            throw new ORCFileException( e.getLocalizedMessage() );
        }
    }
}
