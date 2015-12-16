package voldemort.store.readonly;

import voldemort.VoldemortApplicationException;

public class ReadOnlyFileEntry {

    private final int partitionId;
    private final int chunkId;
    private final int replicaType;

    private final FileType type;
    private final int size;

    public ReadOnlyFileEntry(String name, FileType type, int size) {
        String[] ids = name.split("_", 3);
        if(ids == null || ids.length < 2 || ids.length > 3) {
            throw new VoldemortApplicationException("Could not parse the file name " + name);
        }

        partitionId = Integer.parseInt(ids[0]);
        if(ids.length == 2) {
            replicaType = -1;
            chunkId = Integer.parseInt(ids[1]);
        } else {
            replicaType = Integer.parseInt(ids[1]);
            chunkId = Integer.parseInt(ids[2]);
        }

        this.type = type;
        this.size = size;
    }

    public ReadOnlyFileEntry(String name, FileType type) {
        this(name, type, -1);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + chunkId;
        result = prime * result + partitionId;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        ReadOnlyFileEntry other = (ReadOnlyFileEntry) obj;
        if(chunkId != other.chunkId)
            return false;
        if(partitionId != other.partitionId)
            return false;
        if(type != other.type)
            return false;
        // Size will be -1, for older servers. If any of them is older servers, ignore the size check.
        if(size != -1 && other.size != -1 && size != other.size)
            return false;
        return true;
    }

}
