/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.scheduling;

import cz.muni.fi.mias.indexing.doc.FileExtDocumentHandler;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

/**
 * Class is used to extract and index documents in background thread. To do so
 * class implements {@link Callable} interface. It also tracks runtime of thread
 * so when indexing for this thread is done then this runtime is returned. Task
 * is done when reading of files is done in main thread and queue encapsulated
 * by {@link BackgroundProcessMonitor} is empty.
 *
 * @author Dominik Szalai - emptulik at gmail.com
 */
public class BackgroundTask implements Callable<Long>
{
    private static final Logger LOG = LogManager.getLogger(BackgroundTask.class);
    private final BackgroundProcessMonitor fileProgressMonitor;
    private final IndexWriter indexWriter;

    public BackgroundTask(BackgroundProcessMonitor fileProgressMonitor, IndexWriter indexWriter)
    {
        this.fileProgressMonitor = fileProgressMonitor;
        this.indexWriter = indexWriter;
    }

    @Override
    public Long call() throws Exception
    {
        long start = System.currentTimeMillis();

        while (!fileProgressMonitor.isDoneLoading() || !fileProgressMonitor.getPaths().isEmpty())
        {
            Path path = fileProgressMonitor.getPaths().poll();
            if (path == null)
            {
                LOG.trace("Path queue is empty");
            }
            else
            {
                List<Document> documents = extract(path);
                
                for(Document doc : documents)
                {
                    indexWriter.updateDocument(new Term("id", doc.get("id")), doc);
                }
            }
        }

        return System.currentTimeMillis() - start;
    }

    private List<Document> extract(Path path)
    {
       FileExtDocumentHandler handler = new FileExtDocumentHandler(path, path);
       
       return handler.getDocuments(path, path);
    }

}
