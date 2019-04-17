import com.sun.tools.attach.VirtualMachine;

public class AttachClient {

    public static void main(String[] args) throws Exception {
      String pid = args[0];
      String agent_args = args[1];
        VirtualMachine vm = VirtualMachine.attach(pid);
        System.out.println("vm = " + vm);
	vm.loadAgent("/tmp/jvm-profiler-1.0.0.jar", agent_args);
        vm.detach();
    }
}
