import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class CreateData {

    public static void people(){
        Random rand = new Random(7);
        ArrayList<String> namesList = convertCSVtoArrayList("names.csv");

        File data = new File("PEOPLE.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            int namesIndex = 0;
            for(int i=1; i<=1000000; i++){
                int id = i;
                int x = rand.nextInt(10000);
                int y = rand.nextInt(10000);
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
        ArrayList<String[]> peopleList = convertCSVtoArrayArrayList("PEOPLE.csv");
        File data = new File("ACTIVATED.csv");
        ArrayList<Integer> ids = new ArrayList<>();
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {

            for(int i=1; i<=300000; i++){
                int randId = rand.nextInt(1000000)+1;
                while(true){
                    if(!ids.contains(randId)){
                        ids.add(randId);
                        break;
                    }
                    else
                        randId = rand.nextInt(1000000)+1;
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
        for(int i = 0; i<=1000000; i++){
            hasInfo.add(false);
        }
        ArrayList<String[]> peopleList = convertCSVtoArrayArrayList("PEOPLE.csv");
        ArrayList<String[]> activatedList = convertCSVtoArrayArrayList("ACTIVATED.csv");
        File data = new File("PEOPLE_WITH_HANDSHAKE_INFO.csv");

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

    public static void main(String[]args){
        people();
        activated();
        handshakeInfo();
    }
}
