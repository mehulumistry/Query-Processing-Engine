/** Generated java file from QueryProcessing.java, Main file of Output*/

import java.sql.*;
import java.util.*;
import java.io.PrintWriter;
public class Output {

	public static void main(String[] args) throws Exception {

	 MFStructure[] mfs = new MFStructure[500];
	 ArrayList<MFStructure> mfsList = new ArrayList<>();
	 String usr = "postgres";
	 String url = "jdbc:postgresql://localhost:5432/sales";
	 String pwd = "";
	 Class.forName("org.postgresql.Driver");
	 System.out.println("Successfully loaded the driver!");
	 System.out.println("Successfully connected to the server!");
	 Connection conn = DriverManager.getConnection(url, usr, pwd);
	 Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
	 String query = "select * from sales";
	 ResultSet rs = stmt.executeQuery(query);
	 Scanner input = new Scanner(System.in);
	 System.out.println("Enter the file name for the Output, without extension and By default path: query_output_files/ ");
	 String outputFile = input.nextLine();
	 input.close();
	 PrintWriter writer = new PrintWriter("./query_output_files/"+outputFile.trim() + ".txt", "UTF-8");
 	
 	 int mfsLen = 0;
 	 /** First loop for filling up the Grouping Variables For Eg: cust,prod */

 	 while(rs.next()){
		String cust = rs.getString("cust");
		String prod = rs.getString("prod");
		String state = rs.getString("state");
		int i = 0;
		boolean flag = false;

		while(i<mfsLen){
			if(mfs[i].cust.equals(cust) && mfs[i].prod.equals(prod) && mfs[i].state.equals(state)){
				flag = true;
				
 				break;
			}
			i++;
		}
		if(!flag){
			MFStructure mfstemp = new MFStructure();
			mfstemp.cust = cust;
			mfstemp.prod = prod;
			mfstemp.state = state;

	
			mfs[mfsLen] = mfstemp;
			mfsLen++;
			}
		}
		/** 01 Loop for Gropuing attribute */

		rs.beforeFirst();
		while(rs.next()){
			int i = 0;
			int quant = rs.getInt("quant");
			String cust = rs.getString("cust");
			String prod = rs.getString("prod");
			String state = rs.getString("state");
			int day = rs.getInt("day");
			int month = rs.getInt("month");
			int year = rs.getInt("year");
			while(i<mfsLen){
				if(mfs[i].cust.contains(cust) && mfs[i].prod.contains(prod) && mfs[i].state.contains(state)){
					mfs[i].sum_1_quant += quant;
					mfs[i].count_1_quant += 1;
					try{mfs[i].avg_1_quant = mfs[i].sum_1_quant/mfs[i].count_1_quant;}catch(Exception e){System.out.println(e);}

					}
				i++;
				}
			}
		/** 11 Loop for Gropuing attribute */

		rs.beforeFirst();
		while(rs.next()){
			int i = 0;
			int quant = rs.getInt("quant");
			String cust = rs.getString("cust");
			String prod = rs.getString("prod");
			String state = rs.getString("state");
			int day = rs.getInt("day");
			int month = rs.getInt("month");
			int year = rs.getInt("year");
			while(i<mfsLen){
				if(!mfs[i].cust.contains(cust) && mfs[i].prod.contains(prod) && mfs[i].state.contains(state)){
					mfs[i].sum_2_quant += quant;
					mfs[i].count_2_quant += 1;
					try{mfs[i].avg_2_quant = mfs[i].sum_2_quant/mfs[i].count_2_quant;}catch(Exception e){System.out.println(e);}

					}
				i++;
				}
			}
		/** Having Clause logic */

			/**  No having clause*/
			for(int i= 0;i<mfsLen;i++){
				if(mfs[i] != null){
					mfsList.add(mfs[i]);
				}
			}
		/** Loop for Select Clause for output the result */

		String format = "| %-15s | %-15s | %-15s | %15s | %15s |\n";
		System.out.format(format,"cust","prod","state","avg_1_quant","avg_2_quant");

		writer.format(format,"cust","prod","state","avg_1_quant","avg_2_quant");

		System.out.println('\n');
		writer.println('\n');
		for (MFStructure mfStructure:mfsList){
			try{System.out.format(format,mfStructure.cust,mfStructure.prod,mfStructure.state,mfStructure.avg_1_quant,mfStructure.avg_2_quant);writer.format(format,mfStructure.cust,mfStructure.prod,mfStructure.state,mfStructure.avg_1_quant,mfStructure.avg_2_quant); }
			    catch(Exception e) {
		        continue;
            	}
            }        
		writer.close();
	}
}