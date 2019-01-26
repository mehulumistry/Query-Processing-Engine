/**
 *
 * Developed by: Alisha Singh, Mehul Mistry
 *
 * File Description: Database Connection with postgres using JDBC
 *
 */
import java.sql.*;
import java.util.HashMap;

public class DatabaseConnection {

    public static HashMap<String,String> tableDataTypes = new HashMap<>();

    public static void connect() {
        String usr = "postgres";
        String url = "jdbc:postgresql://localhost:5432/sales";
        String pwd = "";

        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Successfully loaded the driver!");
        } catch (Exception ex) {
            System.out.println("Failed to load the driver!");
            ex.printStackTrace();
        }
        // connection server
        try {
            // opening connection
            Connection conn = DriverManager.getConnection(url, usr, pwd);
            System.out.println("Successfully connected to the server! \n");
            // get query result to ResultSet rs
            Statement stmt = conn.createStatement();
            String query =  "select column_name,data_type from information_schema.columns where table_name='sales'";
            ResultSet rs = stmt.executeQuery(query);

            // create HashMap and store the results in it.
            System.out.println("Creating a HashMap of datatypes from sales datatable....."+"\n");

            while(rs.next()){
                String column_name = rs.getString("column_name");
                String data_type = rs.getString("data_type");
                tableDataTypes.put(column_name,data_type);
            }
           // System.out.println("\n"+ "HashMap Created" + "\n" );
            // traversing through each element
            // closing connection
            conn.close();
        } catch (SQLException ex) {
            System.out.println("Connection URL or username or password errors!");
            ex.printStackTrace();
        }
    }

}


