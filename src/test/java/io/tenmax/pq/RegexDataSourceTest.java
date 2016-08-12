package io.tenmax.pq;

import io.tenmax.poppy.DataFrame;
import io.tenmax.poppy.DataRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class RegexDataSourceTest {

    @Test
    public void testName() throws Exception {

        RegexDataSource ds = new RegexDataSource(
                Arrays.asList(RegexDataSourceTest.class.getResourceAsStream("test.json")),
                "test=(?<test>[a-zA-z]*)&foo=(?<foo>[a-zA-z]*)",
                new String[]{"test", "foo"});

        DataRow row =
        DataFrame
        .from(ds)
        .iterator()
        .next();

        assertEquals(row.getString("test"),"abc");
        assertEquals(row.getString("foo"),"bar");

    }
}