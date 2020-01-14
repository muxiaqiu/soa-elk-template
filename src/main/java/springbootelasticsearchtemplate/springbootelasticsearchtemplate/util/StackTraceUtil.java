package springbootelasticsearchtemplate.springbootelasticsearchtemplate.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Simple utilities to return the stack trace of an exception as a String.
 */
public final class StackTraceUtil {
    public static String getStackTrace(Throwable e) {
        final String NEW_LINE = System.getProperty("line.separator");
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        e.printStackTrace();
        e.printStackTrace(printWriter);
        StringBuilder msg = new StringBuilder()
                .append(e.toString())
                .append(e.getMessage())
                .append(NEW_LINE)
                .append(result.toString());
        return msg.toString();
    }

    public static String getStackTraceEx(Throwable e) {
        final String NEW_LINE = System.getProperty("line.separator");
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            //将出错的栈信息输出到printWriter中
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
        } finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (pw != null) {
                pw.close();
            }
        }
        String exceptionDetail = sw.toString();
        if (exceptionDetail.length() > 3000) {
            exceptionDetail = exceptionDetail.substring(0, 3000) + " ...";
        }
        return exceptionDetail;
    }

    /**
     * Defines a custom format for the stack trace as String.
     */
    public static String getCustomStackTrace(Throwable aThrowable) {
        // add the class name and any message passed to constructor
        final StringBuffer result = new StringBuffer("xxy: ");
        result.append(aThrowable.getMessage());
        final String NEW_LINE = System.getProperty("line.separator");
        result.append(NEW_LINE);
        // add each element of the stack trace
        for (int i = 0; i < aThrowable.getStackTrace().length; i++) {
            StackTraceElement element = aThrowable.getStackTrace()[i];
            result.append(element);
            result.append(NEW_LINE);
        }
        return result.toString();
    }

    /**
     * Demonstrate output.
     */
    public static void main(String[] args) {
        final Throwable throwable = new IllegalArgumentException("Blah");
        System.out.println(getStackTraceEx(throwable));
//        System.out.println(getCustomStackTrace(throwable));
    }
}
