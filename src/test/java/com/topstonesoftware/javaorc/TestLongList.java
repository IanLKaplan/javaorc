package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestLongList extends TestBase {
    private final static long SEED = 127;
    private final static int NUM_ROWS = 2000;
    private final static int MAX_LIST_SIZE = 10;

    @Test
    void longListTest(@TempDir Path tempDirPath) throws ORCFileException {
        Path filePath = tempDirPath.resolve("long_list_data.orc");
        File timestampStringFile = filePath.toFile();
        String filePathStr = timestampStringFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        TypeDescription longListType = TypeDescription.createList(TypeDescription.createLong());
        schema.addField("long_list", longListType);
        List<List<Object>> valueList = new ArrayList<>();
        Random random = new Random(SEED);
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            for (var i = 0; i < NUM_ROWS; i++) {
                row.clear();
                List<Object> listVal = new ArrayList<>();
                int listLen = random.nextInt( MAX_LIST_SIZE ) + 1;
                for (var j = 0; j < listLen; j++) {
                    listVal.add( random.nextLong() );
                }
                valueList.add( listVal );
                row.add(listVal);
                orcWriter.writeRow( row );
            }
        }
        try(var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schema.toString());
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(NUM_ROWS);
            List<Object> row;
            int rowNum = 0;
            while ((row = orcReader.readRow()).size() > 0) {
                assertThat(row.size()).isEqualTo(1);
                Object rowElem = row.get(0);
                assertThat( rowElem ).isInstanceOf( ArrayList.class );
                @SuppressWarnings("unchecked")
                List<Object> longList = (ArrayList<Object>)rowElem;
                assertThat(listsAreEqual(longList, valueList.get( rowNum ))).isTrue();
                rowNum++;
            }
        }
    }

}
