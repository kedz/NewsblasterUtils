package edu.columbia.cs.newsblaster;

import org.apache.commons.cli.*;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.csvreader.CsvWriter;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.FileFilter;
import org.joda.time.DateTime;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 10/27/13
 * Time: 7:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewsblasterReport {

    private AtomicLong totalRuns = new AtomicLong(0);

    private DocumentBuilderFactory dbFactory;
    private DocumentBuilder dBuilder;

    private boolean countFiles = false;

    private HashMap<String, AtomicLong> categoryCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> categoryWordCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> searchCategoryCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> searchCategoryWordCounts = new HashMap<String, AtomicLong>();

    private HashMap<String, AtomicLong> categoryFileCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> categoryFileWordCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> searchCategoryFileCounts = new HashMap<String, AtomicLong>();
    private HashMap<String, AtomicLong> searchCategoryFileWordCounts = new HashMap<String, AtomicLong>();


    private FileUnWrapper fileUnWrapper = new FileUnWrapper();



    private List<Pattern> searchPatterns;


    public NewsblasterReport() {
        dbFactory = DocumentBuilderFactory.newInstance();
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
            System.exit(-1);
        }
    }

    public void processRun(NewsblasterRun run) {


        File todayXml = run.getTodayXml();



        try {

            List<File> cleanFiles = null;
            FileObject cleanArchive = null;
            FileObject[] fileObjects = null;


            if (countFiles) {
                //cleanFiles = fileUnWrapper.extractFiles(run.getCleanFileArchive());

                FileSystemManager fsManager = VFS.getManager();

                if (run.getCleanFileArchive().getName().endsWith("tar.gz"))
                    cleanArchive = fsManager.resolveFile("tgz:"+ run.getCleanFileArchive());
                else
                    cleanArchive = fsManager.resolveFile("tar:"+run.getCleanFileArchive());

                fileObjects = cleanArchive.findFiles(new FileDepthSelector(0,1000000));

            }

            //for (FileObject file : fileObjects)
            //        System.out.println(file.getType().getName());

            //tar:gz:http://anyhost/dir/mytar.tar.gz!/mytar.tar!/path/in/tar/README.txt
            //tgz:file://anyhost/dir/mytar.tgz!/somepath/somefile



            Document doc = dBuilder.parse(todayXml);

            NodeList categories = doc.getElementsByTagName("Category");
            for (int c = 0; c < categories.getLength(); c++) {

                Element category = (Element) categories.item(c);
                String catString = category.getAttribute("name");

                //System.out.println("Category: "+catString);

                NodeList groups = category.getElementsByTagName("Group");
                for (int g = 0; g < groups.getLength(); g++) {

                    boolean groupSearchHit = false;

                    Element group = (Element) groups.item(g);
                    String groupKeywords = group.getAttribute("keywords");

                    //System.out.println("\tGroup: "+groupKeywords);
                    groupSearchHit = searchString(groupKeywords);

                    NodeList events = group.getElementsByTagName("Event");
                    for (int e = 0; e < events.getLength(); e++) {

                        Element event = (Element) events.item(e);
                        String eventTitle = event.getAttribute("title");

                        boolean titleSearchHit = searchString(eventTitle);

                        //System.out.println("\t\tTitle: "+eventTitle);

                        boolean searchHit = false;

                        if (groupSearchHit || titleSearchHit)
                            searchHit = true;

                        NodeList clusters = event.getElementsByTagName("Cluster");
                        for (int cl = 0; cl < clusters.getLength(); cl++) {

                            Element cluster = (Element) clusters.item(cl);
                            String descriptor = cluster.getAttribute("descriptor");

                            //System.out.println("\t\t\tDescriptor: "+descriptor);

                            NodeList summaries = cluster.getElementsByTagName("Summary");

                            //System.out.println("\t\t\t\tNumSums: "+summaries.getLength());
                            incCatCount(catString, summaries.getLength(), searchHit);

                            for (int s = 0; s < summaries.getLength(); s++) {

                                Element summary = (Element) summaries.item(s);

                                NodeList fragments = summary.getElementsByTagName("Fragment");
                                for (int f = 0; f < fragments.getLength(); f++) {

                                    Element fragment = (Element) fragments.item(f);
                                    String fragmentStr = fragment.getAttribute("text");

                                    long wc = fragmentStr.split(" ").length;
                                    incCatWordCount(catString, wc, searchHit);


                                }

                            }

                            NodeList articles = cluster.getElementsByTagName("Article");
                            for (int a = 0; a < articles.getLength(); a++) {

                                Element article = (Element) articles.item(a);
                                Long articleWordCount = Long.parseLong(article.getAttribute("length"));
                                incCatFileWordCount(catString, articleWordCount, searchHit);
                                //String filename = article.getAttribute("file");

                                //countFile(filename, fileObjects, catString, searchHit);


                            }

                            incCatFileCount(catString, articles.getLength(), searchHit);

                        }

                    }




                }

            }


            if (cleanArchive != null) {
                cleanArchive.close();
                //for (FileObject fileObject : fileObjects)
                //    fileObject.close();
            }

            if (cleanFiles != null) {
                for (File file : cleanFiles) {
                    file.delete();
                }
            }

            totalRuns.incrementAndGet();

        } catch (FileSystemException fse) {
            fse.printStackTrace();

        } catch (IOException ioe) {
            ioe.printStackTrace();

        } catch (SAXException saxe) {
            saxe.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
            //System.exit(-1);
        }

    }

    public void countFile(String filename, FileObject[] cleanFiles, String cat, boolean searchHit) {

        FileObject cleanFile = null;

        if (cleanFiles!=null) {

            for (FileObject file : cleanFiles) {

                if (file.getName().toString().endsWith(filename.replaceAll("\\.html", ".txt"))) {

                    cleanFile = file;

                }

            }

            if (cleanFile != null) {

                incCatFileCount(cat, 1, searchHit);
                countFileWords(cat, cleanFile, searchHit);



            } else {
                System.out.println("Could not find file: " + filename);
            }

        } else {
            incCatFileCount(cat, 1, searchHit);
        }
    }

    public void countFileWords(String cat, FileObject article, boolean searchHit) {

        try {

            long wc = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(article.getContent().getInputStream()));

            String line = null;
            while((line=reader.readLine())!=null) {

                wc += line.split(" ").length;

            }
            reader.close();

            incCatFileWordCount(cat, wc, searchHit);



        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }


    public void incCatCount(String cat, long count, boolean searchHit) {

        synchronized (categoryCounts) {
            if (categoryCounts.containsKey(cat)) {

                categoryCounts.get(cat).addAndGet(count);

            } else {

                categoryCounts.put(cat, new AtomicLong(count));

            }
        }

        if (searchHit) {

            synchronized (searchCategoryCounts) {
                if (searchCategoryCounts.containsKey(cat)) {

                    searchCategoryCounts.get(cat).addAndGet(count);

                } else {
                    searchCategoryCounts.put(cat, new AtomicLong(count));
                }
            }

        }

    }

    public void incCatWordCount(String cat, long wc, boolean searchHit) {
        synchronized (categoryWordCounts) {
            if (categoryWordCounts.containsKey(cat)) {

                categoryWordCounts.get(cat).addAndGet(wc);


            } else {
                categoryWordCounts.put(cat, new AtomicLong(wc));
            }
        }

        if (searchHit) {

            synchronized (searchCategoryWordCounts) {
                if (searchCategoryWordCounts.containsKey(cat)) {

                    searchCategoryWordCounts.get(cat).addAndGet(wc);

                } else {
                    searchCategoryWordCounts.put(cat, new AtomicLong(wc));
                }
            }

        }

    }

    public void incCatFileCount(String cat, int count, boolean searchHit) {

        synchronized (categoryFileCounts) {
            if (categoryFileCounts.containsKey(cat)) {

                categoryFileCounts.get(cat).addAndGet(count);


            } else {
                categoryFileCounts.put(cat, new AtomicLong(count));
            }
        }

        if (searchHit) {

            synchronized (searchCategoryFileCounts) {
                if (searchCategoryFileCounts.containsKey(cat)) {

                    searchCategoryFileCounts.get(cat).addAndGet(count);

                } else {
                    searchCategoryFileCounts.put(cat, new AtomicLong(count));
                }
            }

        }

    }

    public void incCatFileWordCount(String cat, long wc, boolean searchHit) {

        synchronized (categoryFileWordCounts) {
            if (categoryFileWordCounts.containsKey(cat)) {

                categoryFileWordCounts.get(cat).addAndGet(wc);

            } else {
                categoryFileWordCounts.put(cat, new AtomicLong(wc));
            }
        }

        if (searchHit) {

            synchronized (searchCategoryFileWordCounts) {
                if (searchCategoryFileWordCounts.containsKey(cat)) {

                    searchCategoryFileWordCounts.get(cat).addAndGet(wc);

                } else {
                    searchCategoryFileWordCounts.put(cat, new AtomicLong(wc));
                }
            }
        }

    }



    public boolean searchString(String searchThis) {

        if (searchPatterns == null)
            return false;

        boolean found = false;

        for (Pattern pattern : searchPatterns) {

            Matcher m = pattern.matcher(searchThis);
            if (m.find()) {
                found = true;
                break;
            }

        }

        return found;

    }

    public AtomicLong getTotalRuns() {
        return totalRuns;
    }


    public void printReport() {

        long total = 0;
        long totalFileCount = 0;
        long totalWordCount = 0;
        long totalFileWordCount = 0;

        System.out.println("\n\nProcessed "+totalRuns.get() +" total runs.\n");

        System.out.println("Summaries per category\n=====================\n");
        for (String cat : categoryCounts.keySet()) {

            AtomicLong catCount = categoryCounts.get(cat);
            System.out.println(cat+": "+catCount);
            total += catCount.get();

        }

        System.out.println("\nTOTAL: "+total);

        System.out.println("\nSummary Word Count per category\n=====================\n");
        for (String cat : categoryWordCounts.keySet()) {

            long catWC = categoryWordCounts.get(cat).get();
            System.out.println(cat+": "+catWC);
            totalWordCount += catWC;

        }
        System.out.println("\nTOTAL: "+totalWordCount);


        System.out.println("\nFiles per category\n=====================\n");
        for (String cat : categoryFileCounts.keySet()) {

            long catCount = categoryFileCounts.get(cat).get();
            System.out.println(cat+": "+catCount);
            totalFileCount += catCount;

        }

        System.out.println("\nTOTAL: "+totalFileCount);

        if (countFiles) {
            System.out.println("\nFile Word Count per category\n=====================\n");
            for (String cat : categoryFileWordCounts.keySet()) {

                long catWC = categoryFileWordCounts.get(cat).get();
                System.out.println(cat+": "+catWC);
                totalFileWordCount += catWC;

            }
            System.out.println("\nTOTAL: "+totalFileWordCount);

        }


        if (searchPatterns != null && searchPatterns.size() > 0) {

            long searchTotalSummaries = 0;
            long searchTotalFiles = 0;
            long searchTotalWordCount = 0;
            long searchTotalFileWordCount = 0;

            System.out.print("\n\nSearch specific stats\n=====================\n\nKeywords:");
            for (Pattern p : searchPatterns) {
                System.out.print(" "+p.toString());
            }
            System.out.println("\n");

            System.out.println("Summaries per category\n=====================\n");
            for (String cat : searchCategoryCounts.keySet()) {

                long catCount = searchCategoryCounts.get(cat).get();
                System.out.println(cat+": "+catCount);
                searchTotalSummaries += catCount;

            }

            System.out.println("\nTOTAL: "+searchTotalSummaries);

            System.out.println("\nSummary Word Count per category\n=====================\n");
            for (String cat : searchCategoryWordCounts.keySet()) {

                long catWC = searchCategoryWordCounts.get(cat).get();
                System.out.println(cat+": "+catWC);
                searchTotalWordCount += catWC;

            }
            System.out.println("\nTOTAL: "+searchTotalWordCount);

            System.out.println("\nFiles per category\n=====================\n");
            for (String cat : searchCategoryFileCounts.keySet()) {

                long catCount = searchCategoryFileCounts.get(cat).get();
                System.out.println(cat+": "+catCount);
                searchTotalFiles += catCount;

            }

            System.out.println("\nTOTAL: "+searchTotalFiles);

            if (countFiles) {
                System.out.println("\nFile Word Count per category\n=====================\n");
                for (String cat : searchCategoryFileWordCounts.keySet()) {

                    long catWC = searchCategoryFileWordCounts.get(cat).get();
                    System.out.println(cat+": "+catWC);
                    searchTotalFileWordCount += catWC;

                }
                System.out.println("\nTOTAL: "+searchTotalFileWordCount);

            }
        }

    }

    public void setSearchStrings(List<String> searchStrings) {

        searchPatterns = new LinkedList<Pattern>();

        for (String searchString : searchStrings) {

            searchPatterns.add( Pattern.compile(Pattern.quote(searchString), Pattern.CASE_INSENSITIVE));


        }

    }

    public void setWorkingDirectory(File workingDirectory) {

        fileUnWrapper.setWorkingDirectory(workingDirectory);

    }

    public void setCountFiles(boolean cf) {
        countFiles = cf;
    }

    public boolean getCountFiles() {
        return countFiles;
    }

    public static void main(String[] args) {

        /* Get command line arguments and set up shop/print usage/otherwise complain */
        CommandLine cmd = null;

        /* Boolean Options */
        Option help = new Option("h", "help", false, "display usage message");

        /* Argument Options */
        Option inputOpt = OptionBuilder.withArgName("Newsblaster Run Archive Directory")
                .hasArg()
                .withDescription("Counts file size for articles in the run archives.")
                .create("d");

        Option singleInputOpt = OptionBuilder.withArgName("Newslbaster Run Directory")
                .hasArg()
                .withDescription("Create report for a single run.")
                .create("f");

        Option workingOpt = OptionBuilder.withArgName("Working Directory")
                .hasArg()
                .withDescription("Location for temporarily unzipping/tarring files.")
                .create("w");

        Option searchOpt = OptionBuilder.withArgName("Search Strings")
                .hasArg()
                .withDescription("Optional comma separated search strings.")
                .create("s");


        Option fileCountOpt = OptionBuilder
                .withDescription("Count articles and  words in articles used to make summaries.")
                .create("c");

        Option beforeDateOpt = OptionBuilder.withArgName("YYYY-MM-DD")
                .hasArg()
                .withDescription("Only report on runs before this date.")
                .create("b");


        Option outputOpt = OptionBuilder.withArgName("Output File")
                .hasArg()
                .withDescription("Name of output csv file.")
                .create("o");

        /* Add Options */
        Options options = new Options();
        options.addOption(help);
        options.addOption(inputOpt);
        options.addOption(singleInputOpt);
        options.addOption(workingOpt);
        options.addOption(outputOpt);
        options.addOption(searchOpt);
        options.addOption(fileCountOpt);
        options.addOption(beforeDateOpt);

        /* Parse command line */
        try {
            CommandLineParser parser = new GnuParser();
            cmd = parser.parse(options, args, false);
        } catch (ParseException pe) {
            System.out.println("Command line options parsing failed. Reason: " + pe.getStackTrace());
            System.exit(1);
        }

        boolean archive = false;
        boolean countFiles = false;
        File inputDir = null;
        File outputFile = null;
        File workingDirectory = null;
        List<String> searchStrings = new LinkedList<String>();

        DateTime beforeDate = null;

        if (cmd.hasOption("b")) {
            beforeDate = DateTime.parse(cmd.getOptionValue("b"));
        }

        if (cmd.hasOption("d")) {
            inputDir = new File(cmd.getOptionValue("d"));
            archive = true;
        } else if (cmd.hasOption("f")) {
            inputDir = new File(cmd.getOptionValue("f"));

        }

        if (cmd.hasOption("c"))
            countFiles = true;

        if (cmd.hasOption("s")) {

            for (String searchStr : cmd.getOptionValue("s").split(",")) {
                searchStrings.add(searchStr.trim());
            }

        }


        if (cmd.hasOption("o")) {
            String outputFilename = cmd.getOptionValue("o");
            if (!outputFilename.endsWith(".csv"))
                outputFilename += ".csv";
            outputFile = new File(outputFilename);
            outputFile.getParentFile().mkdirs();
        }

        if (cmd.hasOption("w")) {
            workingDirectory = new File(cmd.getOptionValue("w"));
        } else {
            workingDirectory = new File(System.getProperty("user.dir")+File.separator+"nbcounter_temp"+File.separator);
        }
        workingDirectory.getParentFile().mkdirs();


        /* Handle command line options */
        if (cmd.hasOption("h") || inputDir == null || outputFile == null) {
            /* Help - print usage and exit */
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NewsblasterReport.class.getName(), options );
            System.exit(0);
        }


        //File workingDirectory = new File("/home/chris/corpora/newsblaster_watson/working_directory");



        NewsblasterRunFactory nbRunFactory = NewsblasterRunFactory.getInstance();

        if (archive) {

            TreeSet<NewsblasterRun> nbRuns = new TreeSet<NewsblasterRun>(new Comparator<NewsblasterRun>() {
                @Override
                public int compare(NewsblasterRun o1, NewsblasterRun o2) {
                    return -(o1.getRunDateTime().compareTo(o2.getRunDateTime()));
                }
            });

            nbRuns.addAll(nbRunFactory.processArchiveDirectory(inputDir));

            NewsblasterReport report = new NewsblasterReport();
            report.setSearchStrings(searchStrings);
            report.setWorkingDirectory(workingDirectory);
            report.setCountFiles(countFiles);

            System.out.println("Processing NB Directory: "+inputDir);

            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());




            for(NewsblasterRun aRun: nbRuns) {

                try {

                    if (beforeDate == null || aRun.getRunDateTime().isBefore(beforeDate))
                        executorService.execute(new RunProcessorThread(aRun, report));





                } catch (Exception e) {
                    System.out.println("Error reading from: " + aRun.getDirectory());
                }

            }


            try {
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

            } catch (InterruptedException ie) {
                ie.printStackTrace();
                System.exit(-1);
            }


            report.printReport();


        } else {

            NewsblasterReport report = new NewsblasterReport();
            report.setSearchStrings(searchStrings);
            report.setWorkingDirectory(workingDirectory);
            report.setCountFiles(countFiles);

            NewsblasterRun run = nbRunFactory.getRunFromDirectory(inputDir);
            if (run != null) {

                System.out.println();
                report.processRun(run);


                report.printReport();

            } else {

                System.out.println("Invalid newsblaster directory.");

            }


        }


    }





}
