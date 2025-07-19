package mars;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.io.File;
import java.io.FileWriter;

/**
 * A simple class that allows any Actor to log the actions taken and the outcomes.
 * Such class manages possible concurrent writes so that the content to be written is not altered
 * but time order might NOT be respected, depending on access time of the Actors but this is a trade-off
 * not solvable as contraining access to also the log function reduced greatly parallelism
 */
public class Logger {
    
    private LocalTime startTime = null;
    private FileWriter writer = null;
    private boolean isStreamOpen = false;

    public String filePath = null;

    public Logger(String filePath){
        startTime = LocalTime.now();
        this.filePath = filePath;

        //try to open file stream
        try {
            //ensure parent directory exists
            File file = new File(filePath);
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }
            
            writer = new FileWriter(filePath);
            isStreamOpen = true;
        } catch (Exception e) {
            System.err.println("It was not possible to open the stream\n" + e);
        }   
    }

    // discouraged to use finalize, to be used as a safety net
    @Override
    protected void finalize() throws Throwable {
        try {
            System.out.println("Closing stream");
            if (isStreamOpen) {
                closeStream();
            }
        } finally {
            super.finalize();
        }
    }

    public boolean isStreamOpen(){
        return this.isStreamOpen;
    }

    public boolean closeStream(){
        try {
            writer.close();
            isStreamOpen = false;
        } catch (Exception e) {
            System.err.println("It was not possible to close the stream");
            return false;
        }
        return true;
    }

    public boolean openStream(){
        try {
            writer = new FileWriter(filePath);
            isStreamOpen = true;
        } catch (Exception e) {
            System.err.println("It was not possible to open the stream");
            return false;
        }
        return true;  
    }

    public boolean log(String actor, String string) {
        try {
            LocalTime now = LocalTime.now();
            long millisTotal = ChronoUnit.MILLIS.between(startTime, now);
            long hours = millisTotal / (1000 * 60 * 60);
            long minutes = (millisTotal / (1000 * 60)) % 60;
            long seconds = (millisTotal / 1000) % 60;
            long millis = millisTotal % 1000;

            StringBuilder localBuilder = new StringBuilder(
                String.format("[%02d:%02d:%02d.%03d] ", hours, minutes, seconds, millis)
            );
            localBuilder.append("[ ").append(actor).append(" ] ").append(string).append("\n");

            //this should avoid concurrent writes on file
            //logs might NOT be time ordered
            synchronized (this) {
                writer.append(localBuilder.toString());
                writer.flush();
            }

        } catch (Exception e) {
            System.err.println("It was not possible to log the message on file");
            return false;
        }
        return true;
    }

}