import java.lang.management.ManagementFactory;
import java.io.File;

public class AttachTarget {

    public static void main(String[] args) throws Exception {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();
        String pid = jvm.substring(0, jvm.indexOf('@'));
        System.out.println("pid: " + pid);
        File pidFile = new File("/tmp/.java_pid" + pid);
        System.out.println("exists: " + pidFile.exists());
        Thread.sleep(600_000);
    }
}
