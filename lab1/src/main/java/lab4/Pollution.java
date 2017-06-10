package lab4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class Pollution {

    private Integer ozone;
    private Integer particullate_matter;
    private Integer carbon_monoxide;
    private Integer sulfure_dioxide;
    private Integer nitrogen_dioxide;
    private Double longitude;
    private Double latitude;
    private Date timestamp;

    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public Pollution(String s) {
        String[] ss = s.split(",");
        this.ozone = Integer.parseInt(ss[0]);
        this.particullate_matter = Integer.parseInt(ss[1]);
        this.carbon_monoxide = Integer.parseInt(ss[2]);
        this.sulfure_dioxide = Integer.parseInt(ss[3]);
        this.nitrogen_dioxide = Integer.parseInt(ss[4]);
        this.longitude = Double.parseDouble(ss[5]);
        this.latitude = Double.parseDouble(ss[6]);
        try {
            this.timestamp = DATE_FORMATTER.parse(ss[7]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Integer getOzone() {
        return ozone;
    }

    public void setOzone(Integer ozone) {
        this.ozone = ozone;
    }

    public Integer getParticullate_matter() {
        return particullate_matter;
    }

    public void setParticullate_matter(Integer particullate_matter) {
        this.particullate_matter = particullate_matter;
    }

    public Integer getCarbon_monoxide() {
        return carbon_monoxide;
    }

    public void setCarbon_monoxide(Integer carbon_monoxide) {
        this.carbon_monoxide = carbon_monoxide;
    }

    public Integer getSulfure_dioxide() {
        return sulfure_dioxide;
    }

    public void setSulfure_dioxide(Integer sulfure_dioxide) {
        this.sulfure_dioxide = sulfure_dioxide;
    }

    public Integer getNitrogen_dioxide() {
        return nitrogen_dioxide;
    }

    public void setNitrogen_dioxide(Integer nitrogen_dioxide) {
        this.nitrogen_dioxide = nitrogen_dioxide;
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

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public static boolean isParsable(String s){

        try{
            String[] ss = s.split(",");
            Integer.parseInt(ss[0]);
            Integer.parseInt(ss[1]);
            Integer.parseInt(ss[2]);
            Integer.parseInt(ss[3]);
            Integer.parseInt(ss[4]);
            Double.parseDouble(ss[5]);
            Double.parseDouble(ss[6]);
            DATE_FORMATTER.parse(ss[7]);
            return true;
        }catch (NumberFormatException | ParseException e){
            return false;
        }

    }

    @Override
    public String toString() {
        return ozone +
                "," + particullate_matter +
                "," + carbon_monoxide +
                "," + sulfure_dioxide +
                "," + nitrogen_dioxide +
                "," + longitude +
                "," + latitude +
                "," + timestamp;
    }
}
