/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.doc;

import cz.muni.fi.mias.Settings;
import cz.muni.fi.mias.indexing.scheduling.BackgroundProcessMonitor;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Dominik Szalai - emptulik at gmail.com
 */
public class MIaSFileVisitor implements FileVisitor<Path>
{
    private static final Logger LOG = LogManager.getLogger(MIaSFileVisitor.class);

    private final BackgroundProcessMonitor fileProgressMonitor;
    private final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*{html,xhtml,zip}");
    private long docLimit = Settings.getDocLimit();;
    private long processed = 0;
    public MIaSFileVisitor(BackgroundProcessMonitor fileProgressMonitor)
    {
        this.fileProgressMonitor = fileProgressMonitor;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
    {
        if (canContinue())
        {
            LOG.trace("Entering directory {}", dir);
            return FileVisitResult.CONTINUE;
        }
        else
        {
            LOG.debug("Document number reached.");
            return FileVisitResult.TERMINATE;
        }
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
    {
        if (canContinue())
        {
            if (matcher.matches(file.getFileName()))
            {
                LOG.trace("Adding file {} to output list.", file);
                fileProgressMonitor.getPaths().offer(file);
                processed++;
                return FileVisitResult.CONTINUE;
            }
            else
            {
                LOG.trace("Path {} was rejected by matcher.",file);
                return FileVisitResult.CONTINUE;
            }
        }
        else
        {
            LOG.debug("Document number reached.");
            return FileVisitResult.TERMINATE;
        }
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
    {
        LOG.error(exc);
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
    {
        LOG.trace("Exiting directory {}", dir);
        return FileVisitResult.CONTINUE;
    }
    
    private boolean canContinue()
    {
        if(docLimit == -1)
        {
            return true;
        }
        
        return processed <= docLimit;
    }
}
