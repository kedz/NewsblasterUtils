package edu.columbia.cs.newsblaster;

import edu.columbia.cs.newsblaster.NewsblasterReport;
import edu.columbia.cs.newsblaster.NewsblasterRun;
import org.apache.commons.vfs2.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.swing.filechooser.FileSystemView;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RunProcessorThread implements Runnable {

    private static AtomicLong numLargeFragments = new AtomicLong(0);
    private NewsblasterRun run;
    private NewsblasterReport report;

    public RunProcessorThread(NewsblasterRun aRun, NewsblasterReport aReport) {
        run = aRun;
        report = aReport;
    }

    public void run() {

        System.out.println("Processing run: " + run.getDirectory());


        try {

            List<File> cleanFiles = null;
            FileObject cleanArchive = null;
            FileObject[] fileObjects = null;

            /*
            if (report.getCountFiles()) {
                //cleanFiles = fileUnWrapper.extractFiles(run.getCleanFileArchive());


                FileSystemManager fsManager = VFS.getManager();
                //synchronized (fsManager) {
                    if (run.getCleanFileArchive().getName().endsWith("tar.gz"))
                        cleanArchive = fsManager.resolveFile("tgz:"+ run.getCleanFileArchive());
                    else
                        cleanArchive = fsManager.resolveFile("tar:"+run.getCleanFileArchive());

                    fileObjects = cleanArchive.findFiles(new FileDepthSelector(0,1000000));
                //}



            }
            */

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            Document doc = dBuilder.parse(run.getTodayXml());

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
                    groupSearchHit = report.searchString(groupKeywords);

                    NodeList events = group.getElementsByTagName("Event");
                    for (int e = 0; e < events.getLength(); e++) {

                        Element event = (Element) events.item(e);
                        String eventTitle = event.getAttribute("title");

                        boolean titleSearchHit = report.searchString(eventTitle);

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

                            //System.report.incCatWordCount(catString, wc, searchHit);out.println("\t\t\t\tNumSums: "+summaries.getLength());
                            report.incCatCount(catString, summaries.getLength(), searchHit);

                            for (int s = 0; s < summaries.getLength(); s++) {

                                Element summary = (Element) summaries.item(s);
                                long summaryWc = 0;
                                NodeList fragments = summary.getElementsByTagName("Fragment");
                                for (int f = 0; f < fragments.getLength(); f++) {

                                    Element fragment = (Element) fragments.item(f);
                                    String fragmentStr = fragment.getAttribute("text");

                                    long wc = fragmentStr.split(" ").length;
                                    summaryWc += wc;
                                    //if (wc > 40)
                                    //    System.out.println("Fragment larger than 40: "+numLargeFragments.incrementAndGet());



                                }

                                System.out.println("Summary WC: "+summaryWc);
                                report.incCatWordCount(catString, summaryWc, searchHit);
                            }


                            NodeList articles = cluster.getElementsByTagName("Article");
                            for (int a = 0; a < articles.getLength(); a++) {

                                Element article = (Element) articles.item(a);
                                Long articleWordCount = Long.parseLong(article.getAttribute("length"));
                                report.incCatFileWordCount(catString, articleWordCount, searchHit);
                                //String filename = article.getAttribute("file");

                                //countFile(filename, fileObjects, catString, searchHit);


                            }

                            report.incCatFileCount(catString, articles.getLength(), searchHit);

                        }

                    }




                }

            }


            if (cleanArchive != null) {
                VFS.getManager().getFilesCache().clear(cleanArchive.getFileSystem());
                for (FileObject fileObject : fileObjects)
                    fileObject.close();
                cleanArchive.close();
                cleanArchive = null;
            }

            if (cleanFiles != null) {
                for (File file : cleanFiles) {
                    file.delete();
                }
            }

            report.getTotalRuns().incrementAndGet();

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

}