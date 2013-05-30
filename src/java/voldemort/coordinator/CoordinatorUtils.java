package voldemort.coordinator;

import org.codehaus.jackson.map.ObjectMapper;

import voldemort.versioning.VectorClock;

public class CoordinatorUtils {

    /**
     * Function to serialize the given Vector clock into a string. If something
     * goes wrong, it returns an empty string.
     * 
     * @param vc The Vector clock to serialize
     * @return The string (JSON) version of the specified Vector clock
     */
    public static String getSerializedVectorClock(VectorClock vc) {
        VectorClockWrapper vcWrapper = new VectorClockWrapper(vc);
        ObjectMapper mapper = new ObjectMapper();
        String serializedVC = "";
        try {
            serializedVC = mapper.writeValueAsString(vcWrapper);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return serializedVC;
    }

    public static VectorClock deserializeVectorClock(String serializedVC) {
        VectorClock vc = null;

        if(serializedVC == null) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();

        try {
            VectorClockWrapper vcWrapper = mapper.readValue(serializedVC, VectorClockWrapper.class);
            vc = new VectorClock(vcWrapper.getVersions(), vcWrapper.getTimestamp());
        } catch(Exception e) {
            e.printStackTrace();
        }

        return vc;
    }
}
