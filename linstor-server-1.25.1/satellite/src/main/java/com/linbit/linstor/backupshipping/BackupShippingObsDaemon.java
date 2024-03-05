package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.api.BackupToObs;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

import com.obs.services.exception.ObsException;

public class BackupShippingObsDaemon implements Runnable, BackupShippingDaemon
{
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private final ErrorReporter errorReporter;
    private final Thread cmdThread;
    private final Thread obsThread;
    private final String[] command;
    private final BackupToObs backupHandler;
    private final String backupName;
    private final ObsRemote remote;
    private final long volSize;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;
    private final Object syncObj = new Object();
    private final byte[] masterKey;

    private boolean running = false;
    private Process cmdProcess;
    private boolean afterTerminationSent = false;
    private boolean doneFirst = true;
    private boolean restore;
    private String uploadId = null;
    private final AccessContext accCtx;

    private final BiConsumer<Boolean, Integer> afterTermination;
    private final ThreadGroup threadGroup;

    public BackupShippingObsDaemon(
            ErrorReporter errorReporterRef,
            ThreadGroup threadGroupRef,
            String threadName,
            String[] commandRef,
            String backupNameRef,
            ObsRemote remoteRef,
            BackupToObs backupHandlerRef,
            boolean restoreRef,
            long size,
            BiConsumer<Boolean, Integer> postActionRef,
            AccessContext accCtxRef,
            byte[] masterKeyRef
    )
    {
        errorReporter = errorReporterRef;
        threadGroup = threadGroupRef;
        command = commandRef;
        remote = remoteRef;
        afterTermination = postActionRef;
        backupName = backupNameRef;
        backupHandler = backupHandlerRef;
        volSize = size;
        accCtx = accCtxRef;
        masterKey = masterKeyRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        cmdThread = new Thread(threadGroupRef, this, threadName);
        restore = restoreRef;

        if (restore)
        {
            obsThread = new Thread(threadGroupRef, this::runRestoring, "backup_" + threadName);
        }
        else
        {
            if (size < 0)
            {
                throw new ImplementationError("Creating a backup must have a positive size, but was: " + size);
            }
            handler.setStdOutListener(false);
            obsThread = new Thread(threadGroupRef, this::runShipping, "backup_" + threadName);
        }
    }

    @Override
    public String start()
    {
        running = true;
        cmdThread.start();
        String uploadIdRef = null;
        try
        {
            if (!restore)
            {
                uploadIdRef = backupHandler.initMultipart(backupName, remote, accCtx, masterKey);
            }
            uploadId = uploadIdRef;
            cmdProcess = handler.startDelimited();
            obsThread.start();
        }
        catch (IOException exc)
        {
            errorReporter.reportError(
                    new SystemServiceStartException(
                            "Unable to daemon for SnapshotShipping",
                            "I/O error attempting to start '" + Arrays.toString(command) + "'",
                            exc.getMessage(),
                            null,
                            null,
                            exc,
                            false
                    )
            );
            shutdown(true);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(
                    new SystemServiceStartException(
                            "Unable to daemon for SnapshotShipping",
                            "Access denied exception attempting to start '" + Arrays.toString(command) + "'",
                            exc.getMessage(),
                            null,
                            null,
                            exc,
                            false
                    )
            );
            shutdown(true);
        }

        return uploadIdRef;
    }

    private void runShipping()
    {
        errorReporter.logTrace("starting sending for backup %s", backupName);
        boolean success = false;
        try
        {
            backupHandler
                    .putObjectMultipart(
                            backupName, cmdProcess.getInputStream(), volSize, uploadId, remote, accCtx, masterKey
                    );
            success = true;
        }
        catch (IOException | StorageException exc)
        {
            if (!success && running)
            {
                // closing the stream might throw exceptions, which do not need to be reported if we already
                // successfully finished previously
                errorReporter.reportError(exc);
            }
        }
        catch (AccessDeniedException exc)
        {
            threadFinished(success, true);
            throw new ImplementationError(exc);
        }
        threadFinished(success, true);
    }

    private void runRestoring()
    {
        errorReporter.logTrace("starting restore for backup %s", backupName);
        boolean success = false;
        try (
                InputStream is = backupHandler.getObject(backupName, remote, accCtx, masterKey);
                OutputStream os = cmdProcess.getOutputStream();
        )
        {
            byte[] readBuf = new byte[1 << 20];
            int readLen = 0;
            while ((readLen = is.read(readBuf)) != -1)
            {
                os.write(readBuf, 0, readLen);
            }
            os.flush();
            Thread.sleep(500);
            success = true;
        }
        catch (IOException | InterruptedException exc)
        {
            if (!success && running)
            {
                // closing the streams might throw exceptions, which do not need to be reported if we already
                // successfully finished previously
                errorReporter.reportError(exc);
            }
        }
        catch (AccessDeniedException exc)
        {
            threadFinished(success, true);
            throw new ImplementationError(exc);
        }
        threadFinished(success, true);
    }

    @Override
    public void run()
    {
        errorReporter.logTrace("starting daemon: %s", Arrays.toString(command));
        boolean success = false;
        boolean first = true;
        while (running)
        {
            Event event;
            try
            {
                event = deque.take();
                if (event instanceof StdOutEvent)
                {
                    errorReporter.logTrace("stdOut: %s", new String(((StdOutEvent) event).data));
                    // should not exist when sending due to handler.setStdOutListener(false)
                }
                else if (event instanceof StdErrEvent)
                {
                    String stdErr = new String(((StdErrEvent) event).data);
                    errorReporter.logWarning("stdErr: %s", stdErr);
                }
                else if (event instanceof ExceptionEvent)
                {
                    errorReporter.logTrace("ExceptionEvent in '%s':", Arrays.toString(command));
                    errorReporter.reportError(((ExceptionEvent) event).exc);
                    // FIXME: Report the exception to the controller
                }
                else if (event instanceof PoisonEvent)
                {
                    errorReporter.logTrace("PoisonEvent");
                    break;
                }
                else if (event instanceof EOFEvent)
                {
                    int exitCode = handler.getExitCode();
                    errorReporter.logTrace("EOF. Exit code: " + exitCode);
                    success = exitCode == 0;
                    if (restore && !first || !restore)
                    {
                        running = false;
                    }
                    else
                    {
                        first = false;
                    }
                }
            }
            catch (InterruptedException exc)
            {
                if (running)
                {
                    errorReporter.reportError(new ImplementationError(exc));
                }
            }
            catch (Exception exc)
            {
                errorReporter.reportError(
                        new ImplementationError(
                                "Unknown exception occurred while executing '" + Arrays.toString(command) + "'",
                                exc
                        )
                );
            }
        }
        threadFinished(success, true);
    }

    private void threadFinished(boolean success, boolean shutdownAllowed)
    {
        synchronized (syncObj)
        {
            if (shutdownAllowed)
            {
                errorReporter.logTrace("thread finished");
            }
            if (doneFirst)
            {
                doneFirst = false;
                if (!success)
                {
                    afterTerminationSent = true;
                    afterTermination.accept(success, null);
                    try
                    {
                        if (uploadId != null)
                        {
                            backupHandler.abortMultipart(backupName, uploadId, remote, accCtx, masterKey);
                        }
                    }
                    catch (ObsException exc)
                    {
                        errorReporter.reportError(exc);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    if (shutdownAllowed)
                    {
                        shutdown(false);
                    }
                }
            }
            else if (!afterTerminationSent)
            {
                afterTerminationSent = true;
                afterTermination.accept(success, null);
                if (!success)
                {
                    try
                    {
                        if (uploadId != null)
                        {
                            backupHandler.abortMultipart(backupName, uploadId, remote, accCtx, masterKey);
                        }
                    }
                    catch (ObsException exc)
                    {
                        errorReporter.reportError(exc);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
            }
        }
    }

    /**
     * Simple event forcing this thread to kill itself
     */
    private static class PoisonEvent implements Event
    {
    }

    @Override
    public void shutdown(boolean needsExtraThread)
    {
        Runnable run = () ->
        {
            threadFinished(false, false);
            running = false;
            handler.stop(false);
            if (cmdThread != null)
            {
                cmdThread.interrupt();
            }
            if (obsThread != null)
            {
                obsThread.interrupt();
            }
            deque.addFirst(new PoisonEvent());
        };
        if (needsExtraThread)
        {
            // needed to prevent deadlock with deviceManager-thread
            new Thread(threadGroup, run, "shutdown_" + backupName).start();
        }
        else
        {
            run.run();
        }
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (cmdThread != null)
        {
            cmdThread.join(timeoutRef);
        }
        if (obsThread != null)
        {
            obsThread.join(timeoutRef);
        }
    }
}
