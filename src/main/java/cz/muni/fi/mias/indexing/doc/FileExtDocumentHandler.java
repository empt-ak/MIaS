package cz.muni.fi.mias.indexing.doc;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Class providing document handling for files based on their file extension.
 * html,xhtml and txt files supported so far.
 *
 * @author Martin Liska
 */
public class FileExtDocumentHandler{

    private static final Logger LOG = LogManager.getLogger(FileExtDocumentHandler.class);
    
    private Path file;
    private Path path;
    private MIasDocumentFactory mIasDocumentFactory = new MIasDocumentFactory();

    public FileExtDocumentHandler(Path file, Path path) {
        this.file = file;
        this.path = path;
    }

    /**
     * Calls coresponding document for input files based on it's extension. If needed, extracts an archive for file entries.
     * HtmlDocument is called in case of xhtml, html and xml files.
     * @param file Input file to be handled.
     * @return List<Lucene> of documents for the input files
     */
    public List<Document> getDocuments(Path file, Path path) {
        String ext = path.getFileName().toString().substring(path.getFileName().toString().lastIndexOf(".") + 1);
        List<Document> result = new ArrayList<>();
        List<MIaSDocument> miasDocuments = new ArrayList<>();
        try {
            ZipFile zipFile;
            if (ext.equals("zip")) {
                zipFile = new ZipFile(file.toFile());
                Enumeration<? extends ZipEntry> e = zipFile.entries();
                while (e.hasMoreElements()) {
                    ZipEntry entry = e.nextElement();
                    if (!entry.isDirectory()) {
                        String name = entry.getName();
                        int extEnd = name.lastIndexOf("#");
                        if (extEnd < name.lastIndexOf(".")) {
                            extEnd = name.length();
                        }
                        ext = name.substring(name.lastIndexOf(".") + 1, extEnd);
                        MIaSDocument miasDocument = mIasDocumentFactory.buildDocument(ext, new ZipEntryDocument(zipFile, path.toString(), entry));
                        if (miasDocument != null) {
                            miasDocuments.add(miasDocument);
                        }
                    }
                }
            } else {
                DocumentSource source = new FileDocument(file.toFile(), path.toString());
                MIaSDocument miasDocument = mIasDocumentFactory.buildDocument(ext, source);
                if (miasDocument!=null) {
                    miasDocuments.add(miasDocument);
                }
            }            
            for (MIaSDocument doc : miasDocuments) {
                result.addAll(doc.getDocuments());
            }
        } catch (IOException ex) {
            LOG.error("Cannot handle file {}", file);
            LOG.error(ex);
        }
        
        return result;
    }
    
}
