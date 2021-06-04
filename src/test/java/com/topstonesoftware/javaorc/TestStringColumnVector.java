package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test for a simple one column ORC file consisting of a string column.
 */
class TestStringColumnVector {

    @Test
    void stringColumnVectorTest(@TempDir Path tempDirPath) throws ORCFileException {
        Path filePath = tempDirPath.resolve("string_vector_data.orc");
        File longVectorFile = filePath.toFile();
        String filePathStr = longVectorFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("my_string", TypeDescription.createString());
        String schemaStr = schema.toString();
        String text = """
                twas brillig and the slithy toves
                did gyre and gimble in the wabe
                all mimsy were the borogroves
                and the moon raths outgrabe
                """;
        String[] words = text.split(" ");

        try (var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            for (String word : words) {
                row.clear();
                row.add(word.trim());
                orcWriter.writeRow(row);
            }
        }
        try (var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schemaStr);
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(words.length);
            List<Object> row;
            int rowNum = 0;
            while ((row = orcReader.readRow()).size() > 0) {
                assertThat(row.size()).isEqualTo(1);
                String word = (String)row.get(0);
                assertThat(word).isEqualTo( words[rowNum].trim());
                rowNum++;
            }
        }
    }

}
