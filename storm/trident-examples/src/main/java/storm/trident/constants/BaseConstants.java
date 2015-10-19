package storm.trident.constants;

/**
 *
 * @author mayconbordin
 */
public interface BaseConstants {
    interface SinkType {
        String FILE = "file";
        String CONSOLE = "console";
    }
    
    interface SourceType {
        String FILE = "file";
        String KAFKA = "kafka";
    }
}
