/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.scheduling;

import cz.muni.fi.mias.Settings;
import cz.muni.fi.mias.indexing.doc.FileExtDocumentHandler;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author Dominik Szalai - emptulik at gmail.com
 */
public class BackgroundTaskHandler
{
    private final BackgroundProcessMonitor fileProgressMonitor;
    private final ExecutorService executorService = Executors.newFixedThreadPool(Settings.getNumThreads() - 1);
    private final List<Future<Long>> tasks = new ArrayList<>(Settings.getNumThreads() - 1);
    private final IndexWriter indexWriter;
    private Long max = Long.valueOf(0);
    private Path rootPath;
    private FileExtDocumentHandler fileExtDocumentHandler = new FileExtDocumentHandler();

    /**
     * Default and the only constructor for BackgroundTaskHandler class.
     *
     * @param fileProgressMonitor monitor holding queue of paths
     * @param indexWriter index writer to be written into
     * @param rootPath 
     */
    public BackgroundTaskHandler(BackgroundProcessMonitor fileProgressMonitor, IndexWriter indexWriter, Path rootPath)
    {
        this.fileProgressMonitor = fileProgressMonitor;
        this.indexWriter = indexWriter;
        this.rootPath = rootPath;
    }

    /**
     * Executing this method creates {@link Settings#getNumThreads() } -1
     * threads. One thread is left for main method, so it does not have to swap
     * with one of these threads.
     */
    public void initThreads()
    {
        for (int i = 0; i < Settings.getNumThreads(); i++)
        {
            tasks.add(executorService.submit(new BackgroundTask(fileProgressMonitor, indexWriter,fileExtDocumentHandler,rootPath)));
        }
    }

    /**
     * Method initializes shutdown of threads created by {@link #initThreads() }
     * method. Method during shutdown also calculates longest running thread.
     *
     * @throws ExecutionException if any error during shutdown occurs
     * @throws InterruptedException if any error during shutdown occurs
     */
    public void shutdown() throws ExecutionException, InterruptedException
    {
        Iterator<Future<Long>> iterator = tasks.iterator();

        while (iterator.hasNext())
        {
            if (iterator.next().isDone())
            {
                max = Math.max(max, iterator.next().get());
                iterator.remove();
            }
        }

        executorService.shutdown();
    }

    /**
     * Method returns runtime of indexing operation.
     * @return indexing runtime
     */
    public Long runtime()
    {
        return max;
    }
}
