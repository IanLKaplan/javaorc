package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestLongColumnVectors {

    /**
     * Write and read an ORC file that consists of two Long columns.
     *
     * @param tempDirPath the temporary file path
     */
    @Test
    void testLongColumnVector(@TempDir Path tempDirPath) throws Exception {
        final int numRows = 2000;
        Path filePath = tempDirPath.resolve("long_vector_data.orc");
        File longVectorFile = filePath.toFile();
        String filePathStr = longVectorFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("longVal_1", TypeDescription.createLong());
        schema.addField("longVal_2", TypeDescription.createLong());
        String schemaStr = schema.toString();
        List<Object> longRow = new ArrayList<>();
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            for (var i = 0; i < numRows; i++) {
                longRow.clear();
                longRow.add((long) i);
                longRow.add((long)(i*2));
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
                assertThat(row.size()).isEqualTo(2);
                Long colOne = (Long)row.get(0);
                Long colTwo = (Long)row.get(1);
                assertThat(colOne).isEqualTo(rowNum);
                assertThat(colTwo).isEqualTo( rowNum * 2L);
                rowNum++;
                assertThat(rowNum).isLessThanOrEqualTo(numRows);
            }
            assertThat(rowNum).isEqualTo(numRows);
        }
    }

}
