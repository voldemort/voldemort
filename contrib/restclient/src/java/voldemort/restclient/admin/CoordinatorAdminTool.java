package voldemort.restclient.admin;

public class CoordinatorAdminTool {

    public static void main(String[] args) {
        try {
            CoordinatorAdminCommand.executeCommand(args);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
