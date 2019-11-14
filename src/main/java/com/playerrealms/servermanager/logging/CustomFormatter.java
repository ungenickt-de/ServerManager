package com.playerrealms.servermanager.logging;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class CustomFormatter extends SimpleFormatter {
    private final Date dat = new Date();
    @Override
    public synchronized String format(LogRecord record) {
       return String.format("[%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp] %2$s: %3$s%n",dat,record.getLevel().getLocalizedName(),formatMessage(record));
    }
}
