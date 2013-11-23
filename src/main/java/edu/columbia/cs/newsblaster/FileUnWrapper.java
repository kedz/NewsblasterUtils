package edu.columbia.cs.newsblaster;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 4/1/13
 * Time: 7:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUnWrapper {

    private File workingDirectory;
    private List<File> trashWhenDone = new LinkedList<File>();


    private static Pattern tarPattern = Pattern.compile("\\.tar", Pattern.CASE_INSENSITIVE);
    private static Pattern gzPattern = Pattern.compile("\\.gz", Pattern.CASE_INSENSITIVE);


    public FileUnWrapper(File workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public FileUnWrapper() {
        this(new File(System.getProperty("user.dir")));
    }


    public List<File> extractFiles(File archiveFile) throws Exception {

        List<File> listOfFiles = new ArrayList<File>();
        //System.out.println(archiveFile);
        if (isZipped(archiveFile)) {
            archiveFile = unzip(archiveFile);
        }

        if (archiveFile != null && isTarred(archiveFile)) {
            listOfFiles.addAll(untar(archiveFile));

        }

        return listOfFiles;
    }

    public File unzip(File zipFile) throws Exception {

        String newName = zipFile.getName();
        newName = newName.substring(0,newName.length()-3);


        File unzippedFile = new File(workingDirectory + File.separator +newName);
        unzippedFile.getParentFile().mkdirs();

        try {
            FileInputStream fin = new FileInputStream(zipFile);
            BufferedInputStream in = new BufferedInputStream(fin);
            FileOutputStream out = new FileOutputStream(unzippedFile);
            GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
            final byte[] buffer = new byte[2048];
            int n = 0;
            while (-1 != (n = gzIn.read(buffer))) {
                out.write(buffer, 0, n);
            }
            out.close();
            gzIn.close();


        } catch (EOFException eof ) {
            System.out.println("Error opening: " +unzippedFile);
            trashWhenDone.add(unzippedFile);
            return null;
            //ioe.printStackTrace();
            //System.exit(1);
        } catch (ZipException ze) {
            System.out.println("Error opening: " +unzippedFile);
            trashWhenDone.add(unzippedFile);
            return null;
        } catch (Exception e) {
            System.out.println("Error opening: " +unzippedFile);
            trashWhenDone.add(unzippedFile);
            return null;
        }

        trashWhenDone.add(unzippedFile);

        return unzippedFile;

    }

    public List<File> untar(File tarFile) throws Exception {

        List<File> unTarredFileList = new LinkedList<File>();

        try {
            TarArchiveInputStream tarInput = new TarArchiveInputStream(new FileInputStream(tarFile));
            TarArchiveEntry entry;

            while (null!=(entry=tarInput.getNextTarEntry())) {
                Pattern filenamePattern = Pattern.compile("/([^/]+)$");
                Matcher m = filenamePattern.matcher(entry.getName());
                if (m.find()) {

                    File outputFile = new File(getWorkingDirectory()+File.separator+m.group(1));




                    if (!outputFile.isDirectory()) {

                        //File outputFile = new File(workingDir+File.separator+".tmp"+File.separator+entry.getFile().getName());
                        outputFile.getParentFile().mkdirs();
                        FileOutputStream out = new FileOutputStream(outputFile);


                        long entrySize = entry.getSize();
                        int bufferSize = 2048;
                        final byte[] buffer = new byte[2048];


                        while(entrySize> 0) {

                            if (entrySize < bufferSize)
                                bufferSize = (int) entrySize;

                            tarInput.read(buffer,0,bufferSize);
                            entrySize -= bufferSize;

                            out.write(buffer);

                        }

                        out.close();

                        if (isTarred(outputFile)||isZipped(outputFile)) {
                            FileUnWrapper unWrapper = new FileUnWrapper(getWorkingDirectory());
                            unTarredFileList.addAll(unWrapper.extractFiles(outputFile));
                            trashWhenDone.addAll(unWrapper.getGarbageList());

                        } else {
                            unTarredFileList.add(outputFile);
                        }

                        trashWhenDone.add(outputFile);

                    }

                }
            }

            tarInput.close();

        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }


        return unTarredFileList;
    }


    public void close() {
        for(File garbage : trashWhenDone)
            garbage.delete();
    }

    public boolean isZipped(File aFile) { return gzPattern.matcher(aFile.getName()).find(); }
    public boolean isTarred(File aFile) { return tarPattern.matcher(aFile.getName()).find(); }

    public List<File> getGarbageList() { return trashWhenDone; }
    public void setWorkingDirectory(File workingDirectory) { this.workingDirectory = workingDirectory; }
    public File getWorkingDirectory() { return workingDirectory; }

}