package com.topstonesoftware.javaorc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test for a two column ORC file where the first column in a timestamp column and the second column in a string column.
 */
class TestTimeStampAndString {

    @Test
    void testTimeStampAndString(@TempDir Path tempDirPath) throws Exception {
        Path filePath = tempDirPath.resolve("timestamp_string_data.orc");
        File timestampStringFile = filePath.toFile();
        String filePathStr = timestampStringFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("timestamp", TypeDescription.createTimestamp());
        schema.addField("string", TypeDescription.createString());
        String timePattern = "HH:mm:ss.SSS";
        SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
        final int numRows = 127;
        List<Pair<Timestamp, String>> valueList = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            for (var i = 0; i < numRows; i++) {
                row.clear();
                Date nowDate = new Date();
                String dateStr = dateFormat.format( nowDate );
                Timestamp timestamp = new Timestamp( nowDate.getTime() );
                row.add( timestamp );
                row.add( dateStr );
                orcWriter.writeRow( row );
                Pair<Timestamp, String> columnPair = new ImmutablePair<>(timestamp, dateStr);
                valueList.add( columnPair );
                Thread.sleep(10);
            } // for
        } // try
        try(var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schema.toString());
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(numRows);
            int rowNum = 0;
            while((row = orcReader.readRow()).size() > 0) {
                Pair<Timestamp, String> rowPair = valueList.get( rowNum );
                Timestamp timestamp = (Timestamp)row.get(0);
                String timeStr = (String)row.get(1);
                assertThat(timestamp).isEqualTo( rowPair.getLeft() );
                assertThat(timeStr).isEqualTo( rowPair.getRight());
                rowNum++;
            }
        }
    }
}
