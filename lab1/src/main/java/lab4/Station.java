package lab4;

public class Station {

    private Double longitude;
    private Double latitude;

    public Station(Double longitude, Double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "Station{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }
}
