package com.topstonesoftware.javaorc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestDoubleAndBigInteger {
    private final static long SEED = 127;
    private final static int NUM_ROWS = 3000;

    @Test
    void doubleAndBigIntegerTest(@TempDir Path tempDirPath) throws ORCFileException {
        Path filePath = tempDirPath.resolve("double_biginteger_data.orc");
        File timestampStringFile = filePath.toFile();
        String filePathStr = timestampStringFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("double_col", TypeDescription.createDouble());
        schema.addField("bigint_col", TypeDescription.createDecimal());
        List<Pair<Double, BigDecimal>> valueList = new ArrayList<>();
        SecureRandom random = new SecureRandom();
        random.setSeed( SEED );
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            for (var i = 0; i < NUM_ROWS; i++) {
                row.clear();
                double doubleVal = random.nextDouble();
                long longVal = random.nextLong();
                BigDecimal bigDecimal = new BigDecimal(longVal);
                Pair<Double, BigDecimal> rowValue = new ImmutablePair<>(doubleVal, bigDecimal);
                valueList.add( rowValue );
                row.add(doubleVal);
                row.add(bigDecimal);
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
                assertThat(row.size()).isEqualTo(2);
                double doubleVal = (Double)row.get(0);
                BigDecimal bigDecimal = (BigDecimal) row.get(1);
                Pair<Double, BigDecimal> rowValue = valueList.get(rowNum);
                assertThat(doubleVal).isEqualTo(rowValue.getLeft());
                assertThat(bigDecimal).isEqualTo(rowValue.getRight());
                rowNum++;
            }
        }
    }

}
