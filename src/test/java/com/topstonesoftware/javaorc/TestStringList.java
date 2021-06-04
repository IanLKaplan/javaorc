package com.topstonesoftware.javaorc;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestStringList extends TestBase {
    private final static String text = """
                When half way through the journey of our life
                I found that I was in a gloomy wood,
                because the path which led aright was lost.
                And ah, how hard it is to say just what
                this wild and rough and stubborn woodland was,
                the very thought of which renews my fear!
                So bitter ’t is, that death is little worse;
                but of the good to treat which there I found,
                I ’ll speak of what I else discovered there.
                I cannot well say how I entered it,
                so full of slumber was I at the moment
                when I forsook the pathway of the truth;
                but after I had reached a mountain’s foot,
                where that vale ended which had pierced my heart
                with fear, I looked on high,
                and saw its shoulders
                mantled already with that planet’s rays
                which leadeth one aright o’er every path.
                Then quieted a little was the fear,
                which in the lake-depths of my heart had lasted
                throughout the night I passed so piteously.
                And even as he who, from the deep emerged
                with sorely troubled breath upon the shore,
                turns round, and gazes at the dangerous water;
                even so my mind, which still was fleeing on,
                turned back to look again upon the pass
                which ne’er permitted any one to live.
                When I had somewhat eased my weary body,
                o’er the lone slope I so resumed my way,
                that e’er the lower was my steady foot.
                Then lo, not far from where the ascent began,
                a Leopard which, exceeding light and swift,
                was covered over with a spotted hide,
                and from my presence did not move away;
                nay, rather, she so hindered my advance,
                that more than once I turned me to go back.
                Some time had now from early morn elapsed,
                and with those very stars the sun was rising
                that in his escort were, when Love Divine
                in the beginning moved those beauteous things;
                I therefore had as cause for hoping well
                of that wild beast with gaily mottled skin,
                the hour of daytime and the year’s sweet season;
                but not so, that I should not fear the sight,
                which next appeared before me, of a Lion,
                — against me this one seemed to be advancing
                with head erect and with such raging hunger,
                that even the air seemed terrified thereby —
                and of a she-Wolf, which with every lust
                seemed in her leanness laden, and had caused
                many ere now to lead unhappy lives.
                The latter so oppressed me with the fear
                that issued from her aspect, that I lost
                the hope I had of winning to the top.
                And such as he is, who is glad to gain,
                and who, when times arrive that make him lose,
                weeps and is saddened in his every thought;
                such did that peaceless animal make me,
                which, ’gainst me coming, pushed me, step by step,
                back to the place where silent is the sun.
                While toward the lowland I was falling fast,
                the sight of one was offered to mine eyes,
                who seemed, through long continued silence, weak.
                When him in that vast wilderness I saw,
                “Have pity on me,” I cried out to him,
                “whate’er thou be, or shade, or very man!”
                “Not man,” he answered, “I was once a man;
                and both my parents were of Lombardy,
                and Mantuans with respect to fatherland.
                ’Neath Julius was I born, though somewhat late,
                and under good Augustus’ rule I lived
                in Rome, in days of false and lying gods.
                I was a poet, and of that just man,
                Anchises’ son, I sang, who came from Troy
                after proud Ilion had been consumed.
                But thou, to such sore trouble why return?
                Why climbst thou not the Mountain of Delight,
                which is of every joy the source and cause?”
                “Art thou that Virgil, then, that fountain-head
                which poureth forth so broad a stream of speech?”
                I answered him with shame upon my brow.
                “O light and glory of the other poets,
                let the long study, and the ardent love
                which made me con thy book, avail me now.
                Thou art my teacher and authority;
                thou only art the one from whom I took
                the lovely manner which hath done me honor.
                Behold the beast on whose account I turned;
                from her protect me, O thou famous Sage,
                for she makes both my veins and pulses tremble!”
                “A different course from this must thou pursue,”
                he answered, when he saw me shedding tears,
                “if from this wilderness thou wouldst escape;
                for this wild beast, on whose account thou criest,
                alloweth none to pass along her way,
                but hinders him so greatly, that she kills;
                and is by nature so malign and guilty,
                that never doth she sate her greedy lust,
                but after food is hungrier than before.
                Many are the animals with which she mates,
                and still more will there be, until the Hound
                shall come, and bring her to a painful death.
                He shall not feed on either land or wealth,
                but wisdom, love and power shall be his food,
                and ’tween two Feltros shall his birth take place.
                Of that low Italy he ’ll be the savior,
                for which the maid Camilla died of wounds,
                with Turnus, Nisus and Eurỳalus.
                And he shall drive her out of every town,
                till he have put her back again in Hell,
                from which the earliest envy sent her forth.
                """;

    @Test
    void stringListTest(@TempDir Path tempDirPath) throws ORCFileException {
        Path filePath = tempDirPath.resolve("long_list_data.orc");
        File timestampStringFile = filePath.toFile();
        String filePathStr = timestampStringFile.getPath();
        TypeDescription schema = TypeDescription.createStruct();
        TypeDescription stringListType = TypeDescription.createList(TypeDescription.createString());
        schema.addField("string_list", stringListType);
        List<List<Object>> valueList = new ArrayList<>();
        List<String> lines = textToLines(text);
        try(var orcWriter = new WriteORCFile(filePathStr, schema)) {
            List<Object> row = new ArrayList<>();
            for (var line : lines) {
                row.clear();
                List<Object> listVal = textToWords(line);
                row.add(listVal);
                valueList.add(listVal);
                orcWriter.writeRow( row );
            }
        }
        try(var orcReader = new ReadORCFile(filePathStr)) {
            String fileSchema = orcReader.getSchema().toString();
            assertThat(fileSchema).isEqualTo(schema.toString());
            long fileRows = orcReader.getNumberOfRows();
            assertThat(fileRows).isEqualTo(lines.size());
            List<Object> row;
            int rowNum = 0;
            while ((row = orcReader.readRow()).size() > 0) {
                assertThat(row.size()).isEqualTo(1);
                Object rowElem = row.get(0);
                assertThat( rowElem ).isInstanceOf( ArrayList.class );
                @SuppressWarnings("unchecked")
                List<Object> stringList = (ArrayList<Object>)rowElem;
                assertThat(listsAreEqual(stringList, valueList.get( rowNum ))).isTrue();
                rowNum++;
            }
        }
    }

}
