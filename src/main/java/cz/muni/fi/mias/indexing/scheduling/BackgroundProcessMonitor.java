/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.scheduling;

import cz.muni.fi.mias.indexing.doc.MIaSFileVisitor;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class is used to hold results of {@link MIaSFileVisitor} class by
 * encapsulating access to {@link ConcurrentLinkedQueue}. Also holds flag
 * whether file reading is done or not.
 *
 * @author Dominik Szalai - emptulik at gmail.com
 */
public class BackgroundProcessMonitor
{
    private final Queue<Path> paths = new ConcurrentLinkedQueue<>();
    private boolean doneLoading = false;
    private AtomicLong docsDone = new AtomicLong(0);

    public boolean isDoneLoading()
    {
        return doneLoading;
    }

    public void setDoneLoading(boolean doneLoading)
    {
        this.doneLoading = doneLoading;
    }

    public Queue<Path> getPaths()
    {
        return paths;
    }

    public AtomicLong getDocsDone()
    {
        return docsDone;
    }
    
    public void docDoneIncrease()
    {
        docsDone.incrementAndGet();
    }
}
