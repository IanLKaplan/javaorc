package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class TestDateColumn {

    /**
     * Test that an exception is thrown if a Date type is written.
     *
     * At the time this test was written, the ORC writer did not correctly write the date epoch value
     * to the ORC file. The value was written as a 32-bit int, instead of a 64 bit long. As a result, the
     * date is incorrect. A timestamp value should be used instead.
     * @param tempDirPath
     * @throws ORCFileException
     * @throws InterruptedException
     */
    @Test
    void dateColumnTest(@TempDir Path tempDirPath) throws ORCFileException, InterruptedException {
        Path filePath = tempDirPath.resolve("date_data.orc");
        File timestampStringFile = filePath.toFile();
        String filePathStr = timestampStringFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("date", TypeDescription.createDate());
        List<Date> valueList = new ArrayList<>();
        try (var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            Date nowDate = new Date();
            row.add(nowDate);
            // The date type is not currently supported
            assertThatThrownBy(() -> {
                orcWriter.writeRow(row);
            }).isInstanceOf(ORCFileException.class);
        } // try
    }

}
