package com.robustdb.raft.log;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class PlainFileLog implements LogStorage{
    private RandomAccessFile raf;
    @Override
    public void openLog() {
        try {
            raf = new RandomAccessFile("/Users/jbiao/IdeaProjects/robustdb/files/log","rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


}
