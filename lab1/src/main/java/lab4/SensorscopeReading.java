package lab4;
import java.util.Comparator;

public class SensorscopeReading{
    private Integer stationID;
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer hour;
    private Integer minute;
    private Integer second;
    private Long epochTime;
    private Integer seqNumber;
    private Double confTime;
    private Double dataTime;
    private Double radioCycle;
    private Double radioPower;
    private Double radioFreq;
    private Double primVolt;
    private Double secVolt;
    private Double solarCurrent;
    private Double globalCurrent;
    private Double energy;

    @Override
    public String toString() {
        return "" + this.stationID.toString() + "," + this.year.toString() + "," + this.month.toString() + "," +
                this.day.toString() + "," + this.hour.toString() + "," + this.minute.toString() + "," +
                this.second.toString() + "," + this.epochTime.toString() + "," + this.seqNumber.toString() + "," +
                this.confTime.toString() + "," + this.dataTime.toString() + "," +
                this.radioCycle.toString() + "," + this.radioPower.toString() + "," + this.radioFreq.toString() + "," +
                this.primVolt.toString() + "," + this.secVolt.toString() + "," + this.solarCurrent.toString() + "," +
                this.globalCurrent.toString() + "," + this.energy.toString() + "\n";
    }

    public static final Comparator<SensorscopeReading> TIME_COMP =
            (r1, r2) -> Long.compare(r1.getEpochTime(), r2.getEpochTime());

    public SensorscopeReading(String s) {
        String[] sf = s.split(" ");
        this.stationID = Integer.parseInt(sf[0]);
        this.year = Integer.parseInt(sf[1]);
        this.month= Integer.parseInt(sf[2]);
        this.day= Integer.parseInt(sf[3]);
        this.hour = Integer.parseInt(sf[4]);
        this.minute = Integer.parseInt(sf[5]);
        this.second = Integer.parseInt(sf[6]);
        this.epochTime = Long.parseLong(sf[7]);
        this.seqNumber = Integer.parseInt(sf[8]);
        this.confTime = Double.parseDouble(sf[9]);
        this.dataTime = Double.parseDouble(sf[10]);
        this.radioCycle = Double.parseDouble(sf[11]);
        this.radioPower = Double.parseDouble(sf[12]);
        this.radioFreq = Double.parseDouble(sf[13]);
        this.primVolt = Double.parseDouble(sf[14]);
        this.secVolt = Double.parseDouble(sf[15]);
        this.solarCurrent = Double.parseDouble(sf[16]);
        this.globalCurrent = Double.parseDouble(sf[17]);
        this.energy = Double.parseDouble(sf[18]);
    }

    public Integer getStationID() {
        return stationID;
    }

    public void setStationID(Integer stationID) {
        this.stationID = stationID;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Integer getMinute() {
        return minute;
    }

    public void setMinute(Integer minute) {
        this.minute = minute;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    public Long getEpochTime() {
        return epochTime;
    }

    public void setEpochTime(Long epochTime) {
        this.epochTime = epochTime;
    }

    public Integer getSeqNumber() {
        return seqNumber;
    }

    public void setSeqNumber(Integer seqNumber) {
        this.seqNumber = seqNumber;
    }

    public Double getConfTime() {
        return confTime;
    }

    public void setConfTime(Double confTime) {
        this.confTime = confTime;
    }

    public Double getDataTime() {
        return dataTime;
    }

    public void setDataTime(Double dataTime) {
        this.dataTime = dataTime;
    }

    public Double getRadioCycle() {
        return radioCycle;
    }

    public void setRadioCycle(Double radioCycle) {
        this.radioCycle = radioCycle;
    }

    public Double getRadioPower() {
        return radioPower;
    }

    public void setRadioPower(Double radioPower) {
        this.radioPower = radioPower;
    }

    public Double getRadioFreq() {
        return radioFreq;
    }

    public void setRadioFreq(Double radioFreq) {
        this.radioFreq = radioFreq;
    }

    public Double getPrimVolt() {
        return primVolt;
    }

    public void setPrimVolt(Double primVolt) {
        this.primVolt = primVolt;
    }

    public Double getSecVolt() {
        return secVolt;
    }

    public void setSecVolt(Double secVolt) {
        this.secVolt = secVolt;
    }

    public Double getSolarCurrent() {
        return solarCurrent;
    }

    public void setSolarCurrent(Double solarCurrent) {
        this.solarCurrent = solarCurrent;
    }

    public Double getGlobalCurrent() {
        return globalCurrent;
    }

    public void setGlobalCurrent(Double globalCurrent) {
        this.globalCurrent = globalCurrent;
    }

    public Double getEnergy() {
        return energy;
    }

    public void setEnergy(Double energy) {
        this.energy = energy;
    }

    public static boolean isParsable(String line){

        try{
            String[] sf = line.split(" ");
            Integer.parseInt(sf[0]);
            Integer.parseInt(sf[1]);
            Integer.parseInt(sf[2]);
            Integer.parseInt(sf[3]);
            Integer.parseInt(sf[4]);
            Integer.parseInt(sf[5]);
            Integer.parseInt(sf[6]);
            Long.parseLong(sf[7]);
            Integer.parseInt(sf[8]);
            Double.parseDouble(sf[9]);
            Double.parseDouble(sf[10]);
            Double.parseDouble(sf[11]);
            Double.parseDouble(sf[12]);
            Double.parseDouble(sf[13]);
            Double.parseDouble(sf[14]);
            Double.parseDouble(sf[15]);
            Double.parseDouble(sf[16]);
            Double.parseDouble(sf[17]);
            Double.parseDouble(sf[18]);
            return true;
        }catch (NumberFormatException ex){
            return false;
        }
    }
}
