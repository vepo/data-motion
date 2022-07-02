package io.vepo.datamotion;

import java.util.Objects;

public class StreamerDefinition {
    private String id;

    private String packageName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamerDefinition that = (StreamerDefinition) o;
        return Objects.equals(id, that.id) && Objects.equals(packageName, that.packageName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, packageName);
    }

    @Override
    public String toString() {
        return String.format("StreamerDefinition[id='%s', packageName='%s']", id, packageName);
    }
}
