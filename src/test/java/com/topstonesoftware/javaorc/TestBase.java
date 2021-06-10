package com.topstonesoftware.javaorc;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class TestBase {

    boolean listsAreEqual(List<Object> a, List<Object> b) {
        boolean listEqual = false;
        if (a.size() == b.size()) {
            listEqual = true;
            for (var i = 0; i < a.size(); i++) {
                Object aElem = a.get(i);
                Object bElem = b.get(i);
                if (aElem != null && bElem != null) {
                    if (!aElem.equals(bElem)) {
                        listEqual = false;
                        break;
                    }
                } else if ((aElem == null && bElem != null) || (aElem != null && bElem == null) ) {
                    listEqual = false;
                    break;
                }
            }
        }
        return listEqual;
    }

    List<String> textToLines(final String text) {
        InputStream istream = IOUtils.toInputStream(text, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader( new InputStreamReader( istream ));
        List<String> lines = new ArrayList<>();
        try {
            while (reader.ready()) {
                String line = reader.readLine().trim();
                if (line.length() > 0) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading string: " + e.getLocalizedMessage());
        }
        return lines;
    }

    List<Object> textToWords(final String text) {
        String[] words = text.split("\\s");
        List<Object> wordList = new ArrayList<>();
        Collections.addAll(wordList, words);
        return wordList;
    }

}
