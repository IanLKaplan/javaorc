package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestLongColumnVectorNull {

    /**
     * Test that both Long and null values can be written to Long column vectors
     *
     * @param tempDirPath the temporary file path
     */
    @Test
    void testLongColumnVectorNull(@TempDir Path tempDirPath) throws Exception {
        final int numRows = 2000;
        Path filePath = tempDirPath.resolve("long_vector_data_with_nulls.orc");
        File longVectorFile = filePath.toFile();
        String filePathStr = longVectorFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("longVal", TypeDescription.createLong());
        String schemaStr = schema.toString();
        List<Object> longRow = new ArrayList<>();
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            for (var i = 0; i < numRows; i++) {
                longRow.clear();
                if ((i & 0x1) != 0) {
                    Long longVal = null;
                    longRow.add(longVal);
                } else {
                    longRow.add((long) i);
                }
                orcWriter.writeRow( longRow );
            }
        }
        try(var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schemaStr);
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(numRows);
            List<Object> row;
            int rowNum = 0;
            while ((row = orcReader.readRow()).size() > 0) {
                assertThat(row.size()).isEqualTo(1);
                Long colOne = (Long)row.get(0);
                if ((rowNum & 0x1) > 0) {
                    assertThat(colOne).isNull();
                } else {
                    assertThat(colOne).isEqualTo(rowNum);
                }
                rowNum++;
                assertThat(rowNum).isLessThanOrEqualTo(numRows);
            }
            assertThat(rowNum).isEqualTo(numRows);
        }
    }

}
