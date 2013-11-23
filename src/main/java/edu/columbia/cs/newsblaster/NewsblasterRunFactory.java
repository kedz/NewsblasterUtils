package edu.columbia.cs.newsblaster;

import org.joda.time.DateTime;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 4/1/13
 * Time: 2:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewsblasterRunFactory {

    private static Pattern datePattern = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)");
    private static Pattern cleanFilesPattern = Pattern.compile("cleanfiles", Pattern.CASE_INSENSITIVE);
    private static Pattern todayXmlPattern = Pattern.compile("today\\.xml", Pattern.CASE_INSENSITIVE);
    //private static Pattern cleanDirPattern = Pattern.compile("clean", Pattern.CASE_INSENSITIVE);


    public static NewsblasterRunFactory NEWSBLASTER_RUN_FACTORY = new NewsblasterRunFactory();

    private NewsblasterRunFactory() {};

    public static NewsblasterRunFactory getInstance() { return NEWSBLASTER_RUN_FACTORY; }

    public List<NewsblasterRun> processArchiveDirectory(File archiveDirectory) {

        //List<NewsblasterRun> nbRuns = new ArrayList<NewsblasterRun>();

        ConcurrentLinkedQueue<NewsblasterRun> nbRuns = new ConcurrentLinkedQueue<NewsblasterRun>();


        NewsblasterDirectoryList nbDirectoryList = new NewsblasterDirectoryList(archiveDirectory);

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


        for(String dateString : nbDirectoryList.keySet()) {

            File runRootDir = nbDirectoryList.get(dateString);

            DateTime runDateTime = NewsblasterDirectoryList.getDateTimeFromDirectory(runRootDir);

            executorService.execute(new NewsblasterRunFactoryWorker(runRootDir, runDateTime, nbRuns));

        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        } catch (InterruptedException ie) {
            ie.printStackTrace();
            System.exit(-1);
        }

        executorService = null;

        List<NewsblasterRun> serialNbRunList = new LinkedList<NewsblasterRun>();
        serialNbRunList.addAll(nbRuns);
        return serialNbRunList;

    }

    private File findCleanFiles(File parentDir) {

        for(File subFile : parentDir.listFiles()) {

            if (subFile.isDirectory()) {

                File cleanFile = findCleanFiles(subFile);
                if (cleanFile != null)
                    return cleanFile;

            } else {
                Matcher m = cleanFilesPattern.matcher(subFile.getName());
                if (m.find()) {
                    return subFile;
                }
            }

        }
        return null;
    }

    private File findTodayXml(File parentDir) {

        for(File subFile : parentDir.listFiles()) {

            if (subFile.isDirectory()) {

                File todayXml = findTodayXml(subFile);
                if (todayXml != null)
                    return todayXml;

            } else {
                Matcher m = todayXmlPattern.matcher(subFile.getName());
                if (m.find()) {
                    return subFile;
                }
            }

        }
        return null;
    }


    private class NewsblasterRunFactoryWorker implements Runnable {

        private DateTime cutoff = DateTime.parse("2003-1-1");
        private File runRootDir;
        private DateTime runDateTime;
        private ConcurrentLinkedQueue<NewsblasterRun> nbRuns;

        public NewsblasterRunFactoryWorker(File runRootDir, DateTime runDateTime, ConcurrentLinkedQueue<NewsblasterRun> nbRuns) {
            this.runRootDir = runRootDir;
            this.runDateTime = runDateTime;
            this.nbRuns = nbRuns;
        }

        public void run() {
            //File cleanFile = findCleanFiles(runRootDir);
            //File todayXml = findTodayXml(runRootDir);


            if (runDateTime.isAfter(cutoff)) {

                File cleanFile = new File(runRootDir, "clean" + File.separator + "cleanfiles.tar.gz");
                cleanFile = (cleanFile.exists()) ? cleanFile : new File(runRootDir, "clean" + File.separator + "cleanfiles.tar");

                File todayXml = new File(runRootDir, "data" + File.separator + "today.xml");

                if (!cleanFile.exists()) {
                    System.out.println(runRootDir + " has no cleanfiles.tar or cleanfiles.tar.gz");
                } else if (!todayXml.exists()) {
                    System.out.println(runRootDir + " has no data/today.xml file.");
                } else {

                    NewsblasterRun aRun = new NewsblasterRun(runRootDir, runDateTime, cleanFile, todayXml);
                    nbRuns.add(aRun);
                    System.out.println("ADDING "+aRun.getRunDateTime());
                }

            }
        }

    }


    public NewsblasterRun getRunFromDirectory(File runDirectory) {

        if (runDirectory.isDirectory()) {
            Matcher m = datePattern.matcher(runDirectory.getName());
            if (m.find()) {
                int year = Integer.parseInt(m.group(1));
                int month = Integer.parseInt(m.group(2));
                int day = Integer.parseInt(m.group(3));
                int hours = Integer.parseInt(m.group(4));
                int minutes = Integer.parseInt(m.group(5));
                int seconds = Integer.parseInt(m.group(6));
                DateTime dateTime = new DateTime(year,month,day,hours,minutes,seconds);

                File cleanFile = findCleanFiles(runDirectory);
                File todayXml = findTodayXml(runDirectory);

                if (cleanFile != null && todayXml != null) {

                    return new NewsblasterRun(runDirectory, dateTime, cleanFile, todayXml);

                }

            }

        }

        return null;

    }

}