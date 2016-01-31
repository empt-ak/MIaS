/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.doc;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;

/**
 * Interface for classes that create Lucene documents.
 * 
 * @author Martin Liska
 */
public interface MIaSDocument {
    
    /**
     * Method returns list of Lucene documents for given MIaSDocument.
     * @return list of lucene documents
     * @throws IOException when any I/O error occurs
     */
    public List<Document> getDocuments() throws IOException;

    public String getLogInfo();
    
}
