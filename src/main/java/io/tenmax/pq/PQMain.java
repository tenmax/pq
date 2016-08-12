package io.tenmax.pq;

import io.tenmax.poppy.DataFrame;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.SpecUtils;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static io.tenmax.poppy.SpecUtils.count;

/**
 * Main class of PQ
 */
public class PQMain {
    private CommandLine commandLine = null;
    private String[] columns;
    private String regex;


    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "pq -R <regular-expression> -c <columnlist> [<file> ...]";
        String header = "\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);

        System.exit(0);
    }

    public PQMain(String[] args) throws Exception {
        parseCommand(args);

        columns = commandLine.getOptionValue("c").split(",");
        regex = commandLine.getOptionValue("R");

        RegexDataSource ds;
        if (commandLine.getArgs().length == 0) {
            ds = new RegexDataSource(
                Arrays.asList(System.in),
                regex,
                columns);
        } else {

            List<InputStream> inputs =
            commandLine.getArgList()
            .stream()
            .map(path -> {
                try {
                    InputStream in = new FileInputStream(path);
                    if (path.endsWith(".gz")) {
                        in = new GZIPInputStream(in);
                    }
                    return in;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());


            ds = new RegexDataSource(
                inputs,
                regex,
                columns
            );
        }

        DataFrame dataFrame = DataFrame.from(ds);

        if (commandLine.hasOption("P")) {
            int p = Integer.parseInt(commandLine.getOptionValue("P"));
            dataFrame = dataFrame.parallel(p);
        }

        if (commandLine.hasOption("g")) {
            dataFrame = dataFrame
            .groupby(commandLine.getOptionValue("g").split(","))
            .aggregate(count().as("c"));
        }

        printResult(dataFrame);
    }

    private void parseCommand(String[] args) {
        Options options = new Options();
        options.addOption("R", true, "the regular expression with groups");
        options.addOption("H", false, "with CSV header");
        options.addOption("P", true, "level of parallelism");
        options.addOption("c", true, "column list. Separated by comma");
        options.addOption("g", true, "group by list. Separated by comma");

        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch(ParseException e) {
            System.out.println("Can't parse arguments: " + e.getMessage());
            printHelp(options);
        }

        if (commandLine.hasOption("h")) {
            printHelp(options);
        }

        if (!commandLine.hasOption("R")) {
            System.err.println("No regular expression specified");
            System.exit(-1);
        }

        if (!commandLine.hasOption("c")) {
            System.err.println("No columns specified");
            System.exit(-1);
        }
    }


    private void printResult(DataFrame dataFrame) throws IOException {
        CSVFormat csvFormat = CSVFormat.DEFAULT;


        if (commandLine.hasOption("H")) {
            csvFormat = csvFormat.withHeader(columns);
        }

        CSVPrinter printer = csvFormat.print(System.out);
        printer.flush();

        for (DataRow row : dataFrame) {
            if (System.out.checkError()) {
                break;
            }

            try {
                printer.printRecord(row);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        printer.close();
    }

    public static void main(String[] args) throws Exception{
        new PQMain(args);
    }
}
