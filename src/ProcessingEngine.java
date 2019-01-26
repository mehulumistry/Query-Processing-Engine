/**
 *
 * Developed by: Alisha Singh, Mehul Mistry
 *
 * File Description: Main file, project starts from here
 * File for database connections and calling QueryProcessing file methods.
 *
 *
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ProcessingEngine {

    public static String[] SELECT_ATTRIBUTES;           // (S)
    public static Integer NUM_OF_GVS;                  // (n)
    public static String[] GROUPING_ATTRIBUTES;         // (V)
    public static String[] F_VECT;                      // ([F])
    public static String[] SUCH_THAT_CONDITIONS;        // [SIGMA])
    public static String[] HAVING_CONDITIONS;           // [G]
    public static String WHERE;                         // where
    public static String inputFileName;
    public static void main(String[] args) throws Exception {

        while (true) {

            QueryProcessing qp = new QueryProcessing();
            Scanner input = new Scanner(System.in);

            List<String> lines = new ArrayList<>();

            String lineNew;
            String outputFile;

            int i = 0;

            int inp = Integer.parseInt(initialInput());

            if(inp == 1){
                System.out.println("Paste the query below! Make your custom query or copy from DemoQueries.txt file:");
                while (i < 7) {
                    lineNew = input.nextLine();
                    System.out.println(lineNew);
                    lines.add(lineNew);
                    i++;
                }
            }
            else if(inp == 2 ){
                System.out.println("Enter the file name without extension you want to take input from, By default path : query_input_files/ , ");
                String inputFile = input.nextLine();
                inputFileName = inputFile;
                try {

                    Scanner s1 = new Scanner(new File("./query_input_files/" + inputFile.trim() + ".txt"));
                    while (i<7){
                        if(s1.hasNext()) {
                            lineNew = s1.nextLine();
                            System.out.println(lineNew);
                            lines.add(lineNew);
                        }
                        else{
                            lines.add("");
                        }
                        i++;

                    }
                    s1.close();
                    System.out.println(lines);
                    System.out.println(lines.size());
                }
                catch(Exception e){
                    System.out.println("File not found!!");
                }


            }
            else{
                System.out.println("By default it will enter into option 1, Paste the query below\n");
                while (i < 7) {
                    lineNew = input.nextLine();
                    System.out.println(lineNew);
                    lines.add(lineNew);
                    i++;
                }
            }

            SELECT_ATTRIBUTES = lines.get(0).split(",");
            NUM_OF_GVS = Integer.valueOf(lines.get(1).trim());
            GROUPING_ATTRIBUTES = lines.get(2).split(",");
            F_VECT = lines.get(3).split(",");
            SUCH_THAT_CONDITIONS = lines.get(4).split(",");
            HAVING_CONDITIONS = lines.get(5).trim().split(",");
            WHERE = lines.get(6).trim();

            System.out.println("having" + HAVING_CONDITIONS);
            System.out.println("where" + WHERE);

            qp.createMFStructure();
            qp.populateMFS();

        }
    }

    public static String initialInput(){
        Scanner input = new Scanner(System.in);
        List<String> lines = new ArrayList<>();
        String lineNew;
        System.out.println("Enter 1: For taking input from console. \n");
        System.out.println("Enter 2: For taking input from file. \n" );
        System.out.println("Enter the number: ");
        lineNew = input.nextLine();
        System.out.println("Thank you for the response! :)\n");
        return lineNew.trim();
    }
}

