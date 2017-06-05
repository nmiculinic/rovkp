package lab4;

/**
 * Created by lpp on 6/5/17.
 */
public class USBabyNameRecord {

    private Integer id;
    private String name;
    private Integer year;
    private String gender;
    private String state;
    private Integer count;

    @Override
    public String toString() {
        return "USBabyNameRecord{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", year=" + year +
                ", gender='" + gender + '\'' +
                ", state='" + state + '\'' +
                ", count=" + count +
                '}';
    }

    public USBabyNameRecord(String s){

        String[] sf = s.split(",");
        this.id = Integer.parseInt(sf[0]);
        this.name = sf[1];
        this.year = Integer.parseInt(sf[2]);
        this.gender = sf[3];
        this.state = sf[4];
        this.count = Integer.parseInt(sf[5]);

    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public static boolean isParsable(String s){

        try{
            String[] sf = s.split(",");
            Integer.parseInt(sf[0]);
            Integer.parseInt(sf[2]);
            Integer.parseInt(sf[5]);
            return true;
        }catch (IndexOutOfBoundsException | NumberFormatException e){
            return false;
        }

    }
}
