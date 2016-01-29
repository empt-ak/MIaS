package cz.muni.fi.mias.indexing;

import cz.muni.fi.mias.PayloadSimilarity;
import cz.muni.fi.mias.Settings;
import cz.muni.fi.mias.indexing.doc.MIaSFileVisitor;
import cz.muni.fi.mias.indexing.scheduling.BackgroundProcessMonitor;
import cz.muni.fi.mias.indexing.scheduling.BackgroundTaskHandler;
import cz.muni.fi.mias.math.MathTokenizer;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Indexing class responsible for adding, updating and deleting files from index,
 * creating, deleting whole index, printing statistics.
 *
 * @author Martin Liska
 * @since 14.5.2010
 */
public class Indexing {

    private static final Logger LOG = LogManager.getLogger(Indexing.class);
    
    private Path indexDirectory;
    private Analyzer analyzer = new StandardAnalyzer();
//    private long docLimit = Settings.getDocLimit();
    private long count = 0;
    private long progress = 0;
    private long fileProgress = 0;
    private Path storageDirectory;
    private long startTime;

    /**
     * Constructor creates Indexing instance. Directory with the index is taken from the Settings.
     *
     */
    public Indexing() {
        this.indexDirectory = Settings.getIndexDir();
    }

    /**
     * Indexes files located in given input path.
     * @param path Path to the documents directory. Can be a single file as well.
     * @param rootDir A path in the @path parameter which is a root directory for the document storage. It determines the relative path
     * the files will be index with.
     */
    public void indexFiles(String path, String rootDir) {
        new Scanner(System.in).next();
        storageDirectory = Paths.get(rootDir);
        final Path documentDirectory = Paths.get(path);
        if (!Files.exists(documentDirectory) || !Files.isReadable(documentDirectory)) {
            LOG.fatal("Document directory '{}' does not exist or is not readable, please check the path.",documentDirectory);            
            System.exit(1);
        }
        try {
            startTime = System.currentTimeMillis();
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_45, analyzer);
            PayloadSimilarity ps = new PayloadSimilarity();
            ps.setDiscountOverlaps(false);
            config.setSimilarity(ps);
            config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            try (IndexWriter writer = new IndexWriter(FSDirectory.open(indexDirectory.toFile()), config))
            {
                BackgroundProcessMonitor fileProgressMonitor = new BackgroundProcessMonitor();
                FileVisitor<Path> fileVisitor = new MIaSFileVisitor(fileProgressMonitor);
                BackgroundTaskHandler taskHandler = new BackgroundTaskHandler(fileProgressMonitor, writer);
                taskHandler.initThreads();
                Files.walkFileTree(documentDirectory, fileVisitor);    
                fileProgressMonitor.setDoneLoading(true);
                
                taskHandler.shutdown();
                LOG.info("Getting list of documents to index.");
//                List<File> files = getDocs(documentDirectory);
//                countFiles(files);
//                LOG.info("Number of documents to index is {}",count);
//                indexDocsThreaded(files, writer);
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            LOG.error(ex);
        }
    }

    private void indexDocsThreaded(List<File> files, IndexWriter writer) {
        try {
            Iterator<File> it = files.iterator();
            ExecutorService executor = Executors.newFixedThreadPool(Settings.getNumThreads());
            Future[] tasks = new Future[Settings.getNumThreads()];
            int running = 0;

            while (it.hasNext() || running > 0) {
                for (int i = 0; i < tasks.length; i++) {
                    if (tasks[i] == null && it.hasNext()) {
                        File f = it.next();
                        String path = null;//resolvePath(f);
//                        Callable callable = new FileExtDocumentHandler(f, path);
                        Callable callable = null;
                        FutureTask ft = new FutureTask(callable);
                        tasks[i] = ft;
                        executor.execute(ft);
                        running++;
                    } else if (tasks[i] != null && tasks[i].isDone()) {
                        List<Document> docs = (List<Document>) tasks[i].get();
                        running--;
                        tasks[i] = null;
                        for (Document doc : docs) {
                            if (doc != null) {
                                if (progress % 10000 == 0) {
                                    printTimes();
                                    writer.commit();
                                }
                                try {
                                    LOG.info("adding to index {} docId={}",doc.get("path"),doc.get("id"));
                                    writer.updateDocument(new Term("id", doc.get("id")), doc);
                                    LOG.info("Documents indexed: {}", ++progress);
                                } catch (Exception ex) {
                                    LOG.fatal("Document '{}' indexing failed: {}",doc.get("path"),ex.getMessage());
                                    LOG.fatal(ex.getStackTrace());
                                }
                            }
                        }
                        LOG.info("File progress: {} of {} done...",++fileProgress, count);
                    }
                }
            }
            printTimes();
            executor.shutdown();
        } catch (IOException | InterruptedException | ExecutionException ex) {
            LOG.fatal(ex);
        }
    }

    /**
     * Optimizes the index.
     */
    public void optimize() {        
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31, analyzer);
        config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());  
        // TODO what do we measure here ? time of optimization or optimiziation
        // and index opening aswell
        startTime = System.currentTimeMillis();
        try(IndexWriter writer = new IndexWriter(FSDirectory.open(indexDirectory.toFile()), config)){
//            writer.optimize();    
            LOG.info("Optimizing time: {} ms",System.currentTimeMillis()-startTime);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Deletes whole current index directory
     */
    public void deleteIndexDir() {
        deleteDir(indexDirectory.toFile());
    }

    private void deleteDir(File f) {
        if (f.exists()) {
            File[] files = f.listFiles();
            for (File file : files)
            {
                if (file.isDirectory())
                {
                    deleteDir(file);
                }
                else
                {
                    file.delete();
                }
            }
            f.delete();
        }
    }

    /**
     * Deletes files located in given path from the index
     *
     * @param path Path of the files to be deleted
     */
    public void deleteFiles(String path) {
        final File docDir = new File(path);
        if (!docDir.exists() || !docDir.canRead()) {
            LOG.error("Document directory '{}' does not exist or is not readable, please check the path.", docDir.getAbsolutePath());
            System.exit(1);
        }
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31, analyzer);
        config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        try(IndexWriter writer = new IndexWriter(FSDirectory.open(indexDirectory.toFile()), config)) { 
            deleteDocs(writer, docDir);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void deleteDocs(IndexWriter writer, File file) throws IOException {
        if (file.canRead()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File file1 : files)
                    {
                        deleteDocs(writer, file1);
                    }
                }
            } else {
                LOG.info("Deleting file {}.",file.getAbsoluteFile());
                writer.deleteDocuments(new Term("path",resolvePath(file.toPath()).toString()));
            }
        }
    }

    /**
     * Prints statistic about the current index
     */
    public void getStats() {
        String stats = "\nIndex statistics: \n\n";
        try(DirectoryReader dr = DirectoryReader.open(FSDirectory.open(indexDirectory.toFile()))) {
            stats += "Index directory: "+indexDirectory + "\n";
            stats += "Number of indexed documents: " + dr.numDocs() + "\n";
            
            long fileSize = 0;
            for (int i = 0; i < dr.numDocs(); i++) {
                Document doc = dr.document(i);
                if (doc.getField("filesize")!=null) {
                    String size = doc.getField("filesize").stringValue();
                    fileSize += Long.valueOf(size);
                }
            }
            
            stats += "Index size: " + Files.size(indexDirectory) + " bytes \n";
            stats += "Approximated size of indexed files: " + fileSize + " bytes \n";

            LOG.info(stats);
        } catch (IOException | NumberFormatException e) {
            LOG.error(e.getMessage());
        } 
    }

    

    private Path resolvePath(Path file) throws IOException {
        return storageDirectory.relativize(file);
    }

    private long getCpuTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long result = 0;
        if (bean.isThreadCpuTimeSupported()) {
            final long[] ids = bean.getAllThreadIds();
            for (long id : ids) {
                result += bean.getThreadCpuTime(id) / 1000000;
            }
        }
        return result;
    }
    
    private long getUserTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long result = 0;
        if (bean.isThreadCpuTimeSupported()) {
            final long[] ids = bean.getAllThreadIds();
            for (long id : ids) {
                result += bean.getThreadUserTime(id) / 1000000;
            }
        }
        return result;
    }

    private void printTimes() {
        LOG.info("---------------------------------");
        LOG.info(Settings.EMPTY_STRING);
        LOG.info("{} DONE in total time {} ms",progress,System.currentTimeMillis() - startTime);
        LOG.info("CPU time {} ms",getCpuTime());
        LOG.info("user time {} ms",getUserTime());
        MathTokenizer.printFormulaeCount(); // TODO
        LOG.info(Settings.EMPTY_STRING);
    }

//    private void countFiles(List<File> files) {
//        if (docLimit > 0) {
//            count = Math.min(files.size(), docLimit);
//        } else {
//            count = files.size();
//        }
//    }
}
