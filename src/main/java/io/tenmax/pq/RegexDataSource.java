package io.tenmax.pq;

import io.tenmax.poppy.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class RegexDataSource implements DataSource<Matcher> {
    private final List<InputStream> ins;
    private final Pattern pattern;
    private final DataColumn[] dataColumns;

    public RegexDataSource(List<InputStream> ins, String regex, String[] columns) {
        this.ins = ins;
        this.dataColumns = new DataColumn[columns.length];
        this.pattern = Pattern.compile(regex);

        int i=0;
        for(String column : columns) {
            dataColumns[i++] = new DataColumn(column, String.class);
        }
    }

    @Override
    public int getPartitionCount() {
        return ins.size();
    }

    @Override
    public Iterator<Matcher> getPartition(int index) {

        return
        new BufferedReader(new InputStreamReader(ins.get(index)))
        .lines()
        .flatMap((line) -> {
            Matcher matcher = pattern.matcher(line);
            if(matcher.find()) {
                return Stream.of(matcher);
            } else {
                return Stream.empty();
            }
        }).iterator();
    }

    @Override
    public DataColumn[] getColumns() {
        return dataColumns;
    }

    @Override
    public Object get(Matcher matcher, String columnName) {
        return matcher.group(columnName);
    }
}
