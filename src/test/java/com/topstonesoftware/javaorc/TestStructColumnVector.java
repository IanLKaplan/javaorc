package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestStructColumnVector extends TestBase {
    private final static String text = """
            Do not go gentle into that good night,
            Old age should burn and rave at close of day;
            Rage, rage against the dying of the light.
            Though wise men at their end know dark is right,
            Because their words had forked no lightning they
            Do not go gentle into that good night.
            Good men, the last wave by, crying how bright
            Their frail deeds might have danced in a green bay,
            Rage, rage against the dying of the light.
            Wild men who caught and sang the sun in flight,
            And learn, too late, they grieved it on its way,
            Do not go gentle into that good night.
            Grave men, near death, who see with blinding sight
            Blind eyes could blaze like meteors and be gay,
            Rage, rage against the dying of the light.
            And you, my father, there on the sad height,
            Curse, bless, me now with your fierce tears, I pray.
            Do not go gentle into that good night.
            Rage, rage against the dying of the light.
            """;

    private TypeDescription buildStructDescription() {
        TypeDescription schema = TypeDescription.createStruct();
        TypeDescription structDef = TypeDescription.createStruct();
        schema.addField("word_num", TypeDescription.createInt());
        structDef.addField("word", TypeDescription.createString());
        structDef.addField("word_hash", TypeDescription.createInt());
        structDef.addField("hash_is_odd", TypeDescription.createBoolean());
        schema.addField("structCol", structDef);
        return schema;
    }

    @Test
    void testStructColumnVector(@TempDir Path tempDirPath) throws ORCFileException {
        Path filePath = tempDirPath.resolve("struct_data.orc");
        File longVectorFile = filePath.toFile();
        String filePathStr = longVectorFile.getPath();
        TypeDescription schema = buildStructDescription();
        List<Object> words = textToWords( text );
        List<List<Object>> rowValues = new ArrayList<>();
        int recordCnt = 0;
        try (var orcWriter = new WriteORCFile(filePathStr, schema)) {
            for (var word : words) {
                List<Object> row = new ArrayList<>();
                List<Object> fieldList = new ArrayList<>();
                row.add(recordCnt);
                fieldList.add(word);
                fieldList.add(word.hashCode());
                fieldList.add( (word.hashCode() & 0x1) == 1 ? Boolean.TRUE : Boolean.FALSE );
                row.add(fieldList);
                orcWriter.writeRow(row);
                rowValues.add(row);
                recordCnt++;
            }
        }
        try(var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schema.toString());
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(recordCnt);
            List<Object> row;
            int rowCnt = 0;
            while ((row = orcReader.readRow()).size() > 0) {
                assertThat(row.size()).isEqualTo(2);
                Object wordCntObj = row.get(0);
                assertThat( wordCntObj ).isInstanceOf( Integer.class );
                assertThat( wordCntObj ).isEqualTo( rowCnt );
                Object recordListObj = row.get(1);
                assertThat( recordListObj).isInstanceOf( ArrayList.class);
                @SuppressWarnings("unchecked")
                List<Object> recordList = (ArrayList<Object>)recordListObj;
                List<Object> rowObject = rowValues.get(rowCnt);
                @SuppressWarnings("unchecked")
                List<Object> rowRecordList = (ArrayList<Object>)rowObject.get(1);
                assertThat(listsAreEqual(recordList, rowRecordList)).isTrue();
                rowCnt++;
            }
        }
    }
}
