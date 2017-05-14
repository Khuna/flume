package ru.svyaznoy.eventagent.utils.filereader;

import java.util.regex.Pattern;

public class PatternEventFinder implements EventFinder{
    private Pattern pattern;

    @Override
    public boolean find(String str) {
        if (pattern.matcher(str).find()) {
            return true;
        } else {
            return false;
        }
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }
}
