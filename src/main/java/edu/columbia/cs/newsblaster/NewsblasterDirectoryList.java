package edu.columbia.cs.newsblaster;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 4/1/13
 * Time: 1:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewsblasterDirectoryList {

    File archiveDirectory;
    HashMap<String,File> newsblasterRunsByDateTimeMap = new HashMap<String, File>();

    static Pattern datePattern = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)");

    public NewsblasterDirectoryList(File archiveDirectory) {

        this.archiveDirectory = archiveDirectory;

        buildDirectoryList(archiveDirectory);



    }

    private void buildDirectoryList(File archiveDirectory) {

        for (File directory : archiveDirectory.listFiles()) {

            if (directory.isDirectory()) {


                DateTime dateTime = getDateTimeFromDirectory(directory);


                if (dateTime!=null) {
                    String dateString = dateTime.getYear()+"-"+dateTime.getMonthOfYear()+"-"+dateTime.getDayOfMonth();

                    if (newsblasterRunsByDateTimeMap.containsKey(dateString)) {

                        DateTime otherTime = getDateTimeFromDirectory(newsblasterRunsByDateTimeMap.get(dateString));
                        if (dateTime.isAfter(otherTime))
                            newsblasterRunsByDateTimeMap.put(dateString, directory);


                    } else {
                        newsblasterRunsByDateTimeMap.put(dateString, directory);
                    }




                } else {

                    System.out.println(directory.getName() + " - Bad directory format.");

                }



            }

        }



    }

    public static DateTime getDateTimeFromDirectory(File dir) {

        if (dir.isDirectory()) {
            Matcher m = datePattern.matcher(dir.getName());
            if (m.find()) {
                int year = Integer.parseInt(m.group(1));
                int month = Integer.parseInt(m.group(2));
                int day = Integer.parseInt(m.group(3));
                int hours = Integer.parseInt(m.group(4));
                int minutes = Integer.parseInt(m.group(5));
                int seconds = Integer.parseInt(m.group(6));
                DateTime dateTime = new DateTime(year,month,day,hours,minutes,seconds);

                return dateTime;
            }



        }

        return null;

    }

    public Set<String> keySet() { return newsblasterRunsByDateTimeMap.keySet(); }
    public File get(String key) { return newsblasterRunsByDateTimeMap.get(key); }

}