package com.cloudwick.kvstore;

import com.cloudwick.kvstore.grpc.LogEntry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * WriteAheadLog (WAL) is the "Hard Drive" of our database.
 * 
 * WHY DO WE NEED THIS? 
 * If our Java program crashes or the power goes out, everything in RAM (like our HashMap)
 * is instantly destroyed. To prevent data loss, the database MUST write every single action
 * to a physical file on the hard drive BEFORE it is allowed to change the RAM.
 * 
 * If a crash happens, we can simply read this file from top to bottom to "replay" history
 * and reconstruct the RAM exactly as it was!
 */
public class WriteAheadLog {
    
    // The physical file on the hard drive (e.g., "kvstore.wal")
    private final File logFile;
    
    // The stream used to append data to the very end of the file.
    private FileOutputStream outputStream;

    /**
     * Constructor: Opens the file or creates it if this is a brand new database.
     */
    public WriteAheadLog(String filePath) throws IOException {
        this.logFile = new File(filePath);
        
        // If the file doesn't exist on the hard drive yet, create a blank one.
        if (!logFile.exists()) {
            logFile.createNewFile();
        }
        
        // Open the file in "Append Mode" (true). 
        // This is crucial! We never want to overwrite old data; we only want to add new lines to the end.
        this.outputStream = new FileOutputStream(logFile, true);
    }

    /**
     * Called every time the Leader wants to save a PUT or DELETE.
     * 
     * WHY `synchronized`? 
     * If 50 users send a PUT request at the exact same millisecond, 50 threads will try
     * to write to this file at once. `synchronized` forces them to form a neat, single-file line.
     */
    public synchronized void append(LogEntry entry) throws IOException {
        // `writeDelimitedTo` is a magic Protobuf method!
        // Because Protobuf compiles to raw binary (1s and 0s), there are no "newlines".
        // This method automatically prefixes the data with its exact size in bytes, 
        // so when we read it later, we know exactly where one message ends and the next begins.
        entry.writeDelimitedTo(outputStream);
        
        // `flush()` forces the Operating System to actually write the data to the physical disk platter.
        // If we didn't do this, the OS might hold it in a temporary RAM buffer to be "efficient",
        // which means a power outage would still cause data loss!
        outputStream.flush(); 
    }

    /**
     * Called ONLY ONCE when the server boots up.
     * It reads the entire file from start to finish to rebuild the database.
     */
    public List<LogEntry> readAll() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        
        // If the file is empty, there is no history to replay.
        if (logFile.length() == 0) {
            return entries;
        }

        try (FileInputStream inputStream = new FileInputStream(logFile)) {
            LogEntry entry;
            
            // `parseDelimitedFrom` reads exactly one message from the binary stream.
            // It returns null when it reaches the end of the file.
            while ((entry = LogEntry.parseDelimitedFrom(inputStream)) != null) {
                entries.add(entry);
            }
        }
        return entries;
    }

    /**
     * Completely rewrites the WAL file from scratch.
     * Used by Followers when the Leader forces them to truncate invalid old logs.
     */
    public synchronized void rewrite(List<LogEntry> logList) throws IOException {
        if (outputStream != null) {
            outputStream.close();
        }
        outputStream = new FileOutputStream(logFile, false);
        for (LogEntry entry : logList) {
            entry.writeDelimitedTo(outputStream);
        }
        outputStream.flush();
    }

    /**
     * Called when the server is shutting down cleanly to close the file handle.
     */
    public synchronized void close() throws IOException {
        if (outputStream != null) {
            outputStream.close();
        }
    }
}
