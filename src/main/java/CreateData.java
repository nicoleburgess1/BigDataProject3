import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class CreateData {

    public static void people(){
        Random rand = new Random(7);
        ArrayList<String> namesList = convertCSVtoArrayList("names.csv");

        File data = new File("TESTPEOPLE.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            int namesIndex = 0;
            for(int i=1; i<=500; i++){
                int id = i;
                int x = rand.nextInt(250);
                int y = rand.nextInt(250);
                String name = namesList.get(namesIndex);;
                int age = rand.nextInt(100);
                String email = name + id + "@gmail.com";
                if(namesIndex==namesList.size()-1)
                    namesIndex=0;
                else
                    namesIndex++;
                writer.println(id + "," + x + "," + y + "," + name + "," + age + "," + email);
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
        }
    }

    public static ArrayList<String> convertCSVtoArrayList(String filePath){
        String line = "";
        String delimiter = ",";
        ArrayList<String> words = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Iterate through the file line by line
            while ((line = br.readLine()) != null) {
                String[] word = line.split(delimiter); //split at comma
                words.addAll(Arrays.asList(word));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }

    public static ArrayList<String[]> convertCSVtoArrayArrayList(String filePath){
        String line = "";
        String delimiter = ",";
        ArrayList<String[]> words = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Iterate through the file line by line
            while ((line = br.readLine()) != null) {
                String[] word = line.split(delimiter); //split at comma
                words.add(word);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }


    public static void activated(){
        Random rand = new Random(7);
        ArrayList<String[]> peopleList = convertCSVtoArrayArrayList("TESTPEOPLE.csv");
        File data = new File("TESTACTIVATED.csv");
        ArrayList<Integer> ids = new ArrayList<>();
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {

            for(int i=1; i<=50; i++){
                int randId = rand.nextInt(500)+1;
                while(true){
                    if(!ids.contains(randId)){
                        ids.add(randId);
                        break;
                    }
                    else
                        randId = rand.nextInt(500)+1;
                }
                writer.println(peopleList.get(randId-1)[0] + "," + peopleList.get(randId-1)[1] + "," + peopleList.get(randId-1)[2] + "," + peopleList.get(randId-1)[3] + "," + peopleList.get(randId-1)[4] + "," + peopleList.get(randId-1)[5]);
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
        }
    }

    public static void handshakeInfo() {
        ArrayList<Boolean> hasInfo = new ArrayList<>();
        for(int i = 0; i<=500; i++){
            hasInfo.add(false);
        }
        ArrayList<String[]> peopleList = convertCSVtoArrayArrayList("TESTPEOPLE.csv");
        ArrayList<String[]> activatedList = convertCSVtoArrayArrayList("TESTACTIVATED.csv");
        File data = new File("TESTPEOPLE_WITH_HANDSHAKE_INFO.csv");

        for(int i=0; i<activatedList.size(); i++){
            hasInfo.set(Integer.parseInt(activatedList.get(i)[0]), true);
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            for(int i=0; i<peopleList.size(); i++){
                String activated = "no";
                if(hasInfo.get(Integer.parseInt(peopleList.get(i)[0]))){
                    activated = "yes";
                }

                writer.println(peopleList.get(i)[0] + "," + peopleList.get(i)[1] + "," + peopleList.get(i)[2] + "," + peopleList.get(i)[3] + "," + peopleList.get(i)[4] + "," + peopleList.get(i)[5] + "," + activated);

            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
        }

    }

    public static void customers() {
        Random rand = new Random(7);
        ArrayList<String> namesList = convertCSVtoArrayList("names.csv");

        File data = new File("Customers.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            int namesIndex = 0;
            for (int i = 1; i <= 50000; i++) {
                int id = i;
                String name = namesList.get(namesIndex);
                int age = rand.nextInt(83) + 18;
                int countryCode = rand.nextInt(500) + 1;
                int salary = rand.nextInt(9999900) + 100;
                if (namesIndex == namesList.size() - 1)
                    namesIndex = 0;
                else
                    namesIndex++;
                writer.println(id + "," + name + "," + age + "," + countryCode + "," + salary);
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");

        }
    }

    public static void purchases() {
        Random rand = new Random(7);
        ArrayList<String> descList = convertCSVtoArrayList("Descriptions.csv");

        File data = new File("Purchases.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            int descIndex = 0;
            for (int i = 1; i <= 5000000; i++) {
                int transID = i;
                int custID = rand.nextInt(50000) + 1;
                int transTotal = rand.nextInt(1981) + 20;
                int transNumItems = rand.nextInt(15) + 1;
                String transDesc = descList.get(descIndex);
                writer.println(transID + "," + custID + "," + transTotal + "," + transNumItems + "," + transDesc);
                if (descIndex == descList.size() - 1)
                    descIndex = 0;
                else
                    descIndex++;
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");

        }
    }
    public static void main(String[]args){
        //people();
        //activated();
        //handshakeInfo();
        customers();
        //purchases();
    }
}
