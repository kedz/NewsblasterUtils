package edu.columbia.cs.newsblaster;

import org.joda.time.DateTime;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 4/1/13
 * Time: 2:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewsblasterRun {

    private File directory;
    private DateTime runDateTime;
    private File cleanFileArchive;
    private File todayXml;

    /*
    private Map<String,ArticleMeta> articleMetaMap;

    private List<FileUnWrapper> unWrapperList = new LinkedList<FileUnWrapper>();
    */

    public NewsblasterRun(File rootDirectory, DateTime runDateTime, File cleanFileArchive, File todayXml) {
        this.directory = rootDirectory;
        this.runDateTime = runDateTime;
        this.cleanFileArchive = cleanFileArchive;
        this.todayXml = todayXml;

        /*
        if (todayXml!=null) {
            articleMetaMap = TodayXmlReader.readTodayXml(todayXml);

        }
        */

    }

    /*
    public List<File> listStories(FileUnWrapper unWrapper) throws Exception {
        unWrapperList.add(unWrapper);
        return unWrapper.extractFiles(getCleanFileArchive());
    }

    public void close() {
        for(FileUnWrapper unWrapper:unWrapperList)
            unWrapper.close();
    }

    public String getCategory(String filename) {

        if (articleMetaMap != null && articleMetaMap.containsKey(filename)) {
            return articleMetaMap.get(filename).getCategory();
        } else {
            return "UNKNOWN";
        }

    }
    */


    public File getCleanFileArchive() { return cleanFileArchive; }
    public File getTodayXml() { return todayXml; }
    public File getDirectory() { return directory; }
    public DateTime getRunDateTime() { return runDateTime; }
}