package voldemort.coordinator;

public class SampleRESTClient {

    public static void main(String[] args) {

        // Create the client
        RESTClient<String, String> clientStore = new RESTClient<String, String>("http://localhost:8080",
                                                                                "test");

        // Sample put
        clientStore.put("a",
                        "Hola Senior !!! Bonjournooo sdafasdfsdfasadf  sadfasdfasdfasdfsad fsad fsadfsadfsF!!!!");

        // Do a sample operation:
        System.out.println("Received response : " + clientStore.get("a"));
    }
}
