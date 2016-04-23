package voldemort.store.readonly;

import voldemort.VoldemortApplicationException;

public class ReadOnlyFileEntry implements Comparable<ReadOnlyFileEntry> {

    @Override
    public int compareTo(ReadOnlyFileEntry other) {
        if(partitionId != other.partitionId) {
            return partitionId - other.partitionId;
        }
        if(chunkId != other.chunkId) {
            return chunkId - other.chunkId;
        }
        if(type != other.type) {
            return type.compareTo(other.type);
        }

        if(size != -1 && other.size != -1) {
            return size - other.size;
        }

        return 0;
    }

    private final int partitionId;
    private final int chunkId;
    private final int replicaType;

    private final FileType type;
    private final int size;

    private final String name;

    public ReadOnlyFileEntry(String name, FileType type, int size) {
        this.name = name;

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

    public int getSize() {
      return size;
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
        return this.compareTo(other) == 0;
    }

    private String getFileName() {
        return name + "." + type.toString().toLowerCase();
    }

    @Override
    public String toString() {
        return "ReadOnlyFile [name=" + getFileName() + ", size=" + size + "]";
    }

}
