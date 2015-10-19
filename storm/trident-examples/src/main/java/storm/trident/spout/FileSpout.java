package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.util.Configuration;
import storm.trident.util.FileUtils;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FileSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);
        
    private File[] files;
    private File currentFile;
    private BufferedReader reader;
    private int curFileIndex = 0;
    private boolean finished = false;
    
    private final String path;
    private final String fieldName;
    private final int maxBatchSize;
    
    private final Map<Long, List<String>> batches = new HashMap<>();

    public FileSpout(int maxBatchSize, String path) {
        this(maxBatchSize, path, "line");
    }

    public FileSpout(int maxBatchSize, String path, String fieldName) {
        this.path = path;
        this.maxBatchSize = maxBatchSize;
        this.fieldName = fieldName;
    }

    @Override
    public void open(Map map, TopologyContext tc) {
        buildIndex();
        openNextFile();
    }
    
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        try {
        List<String> batch = batches.get(batchId);
        
        if (batch == null) {
            if (finished) return;
            batch = new ArrayList<>(maxBatchSize);
            
            for (int i=0; i<maxBatchSize; ) {
                String line = readLine();
                if (!StringUtils.isBlank(line)) {
                    batch.add(line);
                    i++;
                }
            }
            
            batches.put(batchId, batch);
        }
        
        for (String line : batch) {
            collector.emit(new Values(line));
        }
        } catch (IOException ex) {
            LOG.error("Unable to read file " + currentFile.getName(), ex);
        }
    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {
        
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldName);
    }
    
    private void buildIndex() {        
        if (StringUtils.isBlank(path)) {
            LOG.error("The source path has not been set");
            throw new RuntimeException("The source path has to beeen set");
        }
        
        LOG.info("Source path: {}", path);
        
        File dir = new File(path);
        if (!dir.exists()) {
            LOG.error("The source path {} does not exists", path);
            throw new RuntimeException("The source path '" + path + "' does not exists");
        }
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });
        
        LOG.info("Number of files to read: {}", files.length);
    }
    


    /**
     * Read one line from the currently open file. If there's only one file, each
     * instance of the spout will read only a portion of the file.
     * @return The line
     * @throws java.io.IOException
     */
    private String readLine() throws IOException {
        if (finished) return null;
        
        String record = reader.readLine();
        
        if (record == null) {
            if (++curFileIndex < files.length) {
                LOG.info("File {} finished", files[curFileIndex]);
                openNextFile();
                record = reader.readLine();			 
            } else {
                LOG.info("No more files to read");
                finished = true;
            }
        }
        return record;
    }

    /**
     * Opens the next file from the index. If there's multiple instances of the
     * spout, it will read only a portion of the files.
     */
    private void openNextFile() {
        try {
            currentFile = files[curFileIndex];
            reader = new BufferedReader(new FileReader(currentFile));
            LOG.info("Opened file {}, size {}", currentFile.getName(), 
                    FileUtils.humanReadableByteCount(currentFile.length()));
            
        } catch (FileNotFoundException e) {
            LOG.error(String.format("File %s not found", currentFile), e);
            throw new IllegalStateException("file not found");
        }
    }
}	
