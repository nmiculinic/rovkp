package lab4;

import java.io.Serializable;

/**
 *
 * @author Martina
 * c/p from ferweb
 */
public class USDeathRecord implements Serializable {

    private final int monthOfDeath, age, dayOfWeekOfDeath, mannerOfDeath;
    private final String sex, maritalStatus, autopsy;


    public USDeathRecord(String line) throws NumberFormatException {
        String[] splitted = line.split(",");
        monthOfDeath = Integer.parseInt(splitted[5]);
        sex = splitted[6];
        age = Integer.parseInt(splitted[8]);
        maritalStatus = splitted[15];
        dayOfWeekOfDeath = Integer.parseInt(splitted[16]);
        mannerOfDeath = Integer.parseInt(splitted[19]);
        autopsy = splitted[21];
    }

    public int getMonthOfDeath() {
        return monthOfDeath;
    }

    public String getGender() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public String getMaritialStatus() {
        return maritalStatus;
    }

    public int getDayOfWeekOfDeath() {
        return dayOfWeekOfDeath;
    }

    public int getMannerOfDeath() {
        return mannerOfDeath;
    }

    public String getAutopsy() {
        return autopsy;
    }


    public static boolean isParsable(String line) {
        try {
            String[] splitted = line.split(",");
            Integer.parseInt(splitted[5]);
            Integer.parseInt(splitted[8]);
            Integer.parseInt(splitted[16]);
            Integer.parseInt(splitted[19]);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }


}