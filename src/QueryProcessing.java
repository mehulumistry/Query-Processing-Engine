/**
 * Developed by: Alisha Singh, Mehul Mistry
 * <p>
 * File Description: File for generating Output.java, MFStructure.java file
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

class QueryProcessing {

    /**
     * Creating the Main Output.java file which will produce the final result
     */

    File file = new File("./src/Output.java");;
    FileWriter writer;
    HashMap<String, String> tdatatypes = DatabaseConnection.tableDataTypes;
    ArrayList<String> fVects = new ArrayList<>();

    /**
     * function creates MF structure and throws expection at method level
     * 1st part of the project
     * <p>
     * Logic: looping through grouping attributes and check the HashMap for the data_type and feed it into MF_Structure
     * <p>
     * Intuition: First write the Output.java that what you want and get the result and then build the logic in QueryProcessing.java
     * Assumption: F_Vect will only contain aggregate elements, when looping through select statement add the columns that is not in Grouping Variable.
     */

    public void createMFStructure() throws IOException {

        // Uncomment the below to save the generated file in the GeneratedOutput files folder

//        System.out.println("sdf" + ProcessingEngine.inputFileName);
//        File dir = new File("./GeneratedOutputFiles/" +ProcessingEngine.inputFileName);
//        dir.mkdirs();
//        file = new File(dir, "Output.java");



        System.out.println("Creating MF Structure class....");

        // Uncomment the below to save the generated file in the GeneratedOutput files folder
       // File mfsFile = new File(dir,"MFStructure.java");
        File mfsFile = new File("./src/MFStructure.java");
        FileWriter mfsWriter = new FileWriter(mfsFile);
        mfsWriter.write("/** Generated java file from QueryProcessing.java, MFStructure class  */\n\n");
        mfsWriter.write("class MFStructure{" + "\n");
        System.out.println("MF Structure building....");
        writer = new FileWriter(file);
        String importOut = "/** Generated java file from QueryProcessing.java, Main file of Output*/\n\n" + "import java.sql.*;\n" + "import java.util.*;\n" + "import java.io.PrintWriter;\n";
        String template = "public class Output {" + "\n\n" + "\t" + "public static void main(String[] args) throws Exception {" + "\n\n";
        writer.write(importOut + template);
        try {
            //Establishing the connection for the first time to get the datatypes of the column for MF Structure
            DatabaseConnection.connect();

            // For the grouping Attributes
            for (String groupingAttribute : ProcessingEngine.GROUPING_ATTRIBUTES) {
                groupingAttribute = groupingAttribute.trim();
                // System.out.println("Grouping Attribute " + groupingAttribute.trim());
                if (tdatatypes.containsKey(groupingAttribute)) {
                    if (tdatatypes.get(groupingAttribute.trim()).contains("character")) {
                        mfsWriter.write("\t" + "public String " + groupingAttribute + ";" + "\n");
                        //System.out.println("public String " + groupingAttribute + ";");
                    } else if (tdatatypes.get(groupingAttribute.trim()).contains("integer")) {
                        mfsWriter.write("\t" + "public int " + groupingAttribute + ";" + "\n");
                        //System.out.println("public int " + groupingAttribute + ";");
                    }
                }
            }

            String[] fVectDummy = ProcessingEngine.F_VECT;

            // making of F-Vect
            for(int i =0; i<fVectDummy.length;i++){
                fVects.add(fVectDummy[i].trim());
            }

            ArrayList<String> tempFvect = new ArrayList<>();
            // F-Vect
            for (String fVect : fVects) {
                fVect = fVect.trim();
                if (fVect.startsWith("avg_")) {
                    String[] fVectA = fVect.split("_");
                    if (fVectA.length == 2) {
                        tempFvect.add("sum_" + fVectA[1]);
                        tempFvect.add("count_" + fVectA[1]);
                    } else if (fVectA.length == 3) {
                        tempFvect.add("sum_" + fVectA[1] + "_" + fVectA[2]);
                        tempFvect.add("count_" + fVectA[1] + "_" + fVectA[2]);
                    }
                }
            }

            fVects.addAll(tempFvect);

            //  F_VECT Role in creating MF_Structure, Find Avg and add other two columns of sum and count
            for (String fVect : fVects) {
                mfsWriter.write("\t" + "public int " + fVect.trim() + ";" + "\n");
            }

            mfsWriter.write("}");
        } catch (Exception e) {
            System.out.println("Table Datatype is not created, Check SQL connection" + e);
        }

        if (file.createNewFile()) {
            System.out.println("File is created!");
        } else {
            System.out.println("File already exists.");
        }

        System.out.println("MFStructure created, MFStructure.java file created\n");
        // File writing done, Closing connection

        // create MFS class in Output.java
        writer.write("\t" + " MFStructure[] mfs = new MFStructure[500];" + "\n");
        writer.write("\t ArrayList<MFStructure> mfsList = new ArrayList<>();\n");
        mfsWriter.close();
    }


    /*************** 2.1) Filling the grouping variables of MF Structure *****************
     * 2nd part of the project
     * Steps:
     *      1) Establishing database connection for 2nd time for Table Scans and filling the data in MFStructure
     *      2) Write the code in Output.java
     *      3) Creating a datastructure to store the MF Table. Creating a array which will store the instance of MFStructure
     *      4) Start with filling the data for grouping attributes. For Eg. cust, prod
     *      5) Check whether "Where" condition exists or not. if exist then  update the query
     */


    public void populateMFS() throws Exception {

        // 1) Establish the connection with database in Output.java file

        String queryMFS = "";
        String where = ProcessingEngine.WHERE.trim();
        if (where.length() > 2) {
            queryMFS = "select * from sales " + where;
        } else {
            queryMFS = "select * from sales";
        }
        String dbConnection = "";
        dbConnection += "\t" + " String usr = \"postgres\";" + "\n";
        dbConnection += "\t" + " String url = \"jdbc:postgresql://localhost:5432/sales\";" + "\n";
        dbConnection += "\t" + " String pwd = \"\";" + "\n";
        dbConnection += "\t" + " Class.forName(\"org.postgresql.Driver\");" + "\n";
        dbConnection += "\t" + " System.out.println(\"Successfully loaded the driver!\");" + "\n";
        dbConnection += "\t" + " System.out.println(\"Successfully connected to the server!\");" + "\n";
        dbConnection += "\t" + " Connection conn = DriverManager.getConnection(url, usr, pwd);" + "\n";
        dbConnection += "\t" + " Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE," + "ResultSet.CONCUR_READ_ONLY);" + "\n";
        dbConnection += "\t" + " String query = " + '"' + queryMFS + '"' + ";\n";
        dbConnection += "\t" + " ResultSet rs = stmt.executeQuery(query);" + "\n";
        writer.write(dbConnection);
        System.out.println("Connection Established\n");

        // 2) First While loop for filling up the data of grouping variables.

        System.out.println("Filling Grouping Attributes started.....\n");
        String gvFilling = "";
        String gvfinner = "";
        String gvfcond = "";
        String gvfResult = "";
        ArrayList<String> gv0 = new ArrayList<>();
        ArrayList<String> gvN = new ArrayList<>();
        ArrayList<String> mfsDataTypes = new ArrayList<>();
        mfsDataTypes.addAll(fVects);

        for (String aF_VECT : fVects) {
            aF_VECT = aF_VECT.trim();
            int numberOfUS = aF_VECT.length() - aF_VECT.replace("_", "").length();

            if (numberOfUS == 1) {
                String temp = aF_VECT.split("_")[1];
                if (!gv0.contains(temp)) {
                    gv0.add(temp);
                }
            } else if (numberOfUS > 1) {
                String temp = aF_VECT.split("_")[2];
                if (!gvN.contains(temp)) {
                    gvN.add(temp);
                }
            }
        }

        ////////////  For grouping attribute 0  /////////////
        String gv0FAi = "";
        String gv0FA = "";
        String newgv0FA = "";

        if (gv0.size() > 0) {
            for (String gv : gv0) {
                if (mfsDataTypes.contains("sum_" + gv)) {
                    gv0FAi += " int sum_" + gv + " = " + 0 + ";\n";
                    gv0FA += "\t\t" + "sum_" + gv + " = " + "rs.getInt(\"" + gv + "\");\n";
                    gv0FA += "\t\t\t" + "mfs[i].sum_" + gv + " += " + "sum_" + gv + ";\n";
                    newgv0FA += "\t\t" + "sum_" + gv + " = " + "rs.getInt(\"" + gv + "\");\n";
                    newgv0FA += "\t\tmfstemp.sum_" + gv + "=" + "sum_" + gv + ";\n";
                }
                if (mfsDataTypes.contains("count_" + gv)) {
                    gv0FAi += " int count_" + gv + " = " + 0 + ";\n";
                    gv0FA += "\t\t\t" + "mfs[i].count_" + gv + " += 1 " + ";\n";
                    newgv0FA += "\t\t" + "count_" + gv + " = " + 1 + ";\n";
                    newgv0FA += "\t\tmfstemp.count_" + gv + "=" + "count_" + gv + ";\n";
                }
                if (mfsDataTypes.contains("avg_" + gv)) {
                    gv0FAi += " int avg_" + gv + " = " + 0 + ";\n";
                    gv0FA += "\t\t" + "try{avg_" + gv + " = " + "mfs[i].sum_" + gv + "/" + "mfs[i].count_" + gv + ";}catch(Exception e) {System.out.println(e);}\n";
                    gv0FA += "\t\t\t" + "mfs[i].avg_" + gv + " = " + "avg_" + gv + ";\n";
                    newgv0FA += "\t\t" + "try{mfstemp.avg_" + gv + " = " + "mfstemp.sum_" + gv + "/" + "mfstemp.count_" + gv + ";}catch(Exception e) {System.out.println(e);}\n";
                }
                if (mfsDataTypes.contains("max_" + gv)) {
                    System.out.println("hello");
                    gv0FAi += " int max_" + gv + " = " + 0 + ";\n";
                    gv0FA += "\t\t" + "if(max_" + gv + " < " + "rs.getInt(\"" + gv + "\")" + ")" + "max_" + gv + "=" + "rs.getInt(\"" + gv + "\")" + ";\n";
                    gv0FA += "\t\t\t" + "mfs[i].max_" + gv + " = " + "max_" + gv + ";\n";
                    newgv0FA += "mfstemp.max_" + gv + "=rs.getInt(\"" + gv + "\");";
                }
                if (mfsDataTypes.contains("min_" + gv)) {
                    gv0FAi += " int min_" + gv + " = " + 0 + ";\n";
                    gv0FA += "\t\t" + "if(min_" + gv + " > " + "rs.getInt(\"" + gv + "\")" + ")" + "min_" + gv + "=" + "rs.getInt(\"" + gv + "\")" + ";\n";
                    gv0FA += "\t\t\t" + "mfs[i].min_" + gv + " = " + "min_" + gv + ";\n";
                    newgv0FA += "mfstemp.min_" + gv + "=rs.getInt(\"" + gv + "\");";
                }
            }
        }

        gvFilling += " \t" + gv0FAi + "\n" + " \t int mfsLen = 0;\n" + " \t /** First loop for filling up the Grouping Variables For Eg: cust,prod */\n\n" +
                " \t while(rs.next()){\n";
        String[] gvAttributes = ProcessingEngine.GROUPING_ATTRIBUTES;
        for (int i = 0; i < gvAttributes.length; i++) {
            // if grouping attribute is integer
            gvAttributes[i] = gvAttributes[i].trim();
            if (tdatatypes.get(gvAttributes[i]).equals("integer")) {
                gvfinner += "\t\tint " + gvAttributes[i] + " = rs.getInt(\"" + gvAttributes[i] + "\");\n";
                if (i >= gvAttributes.length - 1) {
                    gvfcond += "mfs[i]." + gvAttributes[i] + "==" + gvAttributes[i];
                } else {
                    gvfcond += "mfs[i]." + gvAttributes[i] + "==" + gvAttributes[i] + " && ";
                }
            } else {
                gvfinner += "\t\tString " + gvAttributes[i] + " = rs.getString(\"" + gvAttributes[i] + "\");\n";
                if (i >= gvAttributes.length - 1) {
                    gvfcond += "mfs[i]." + gvAttributes[i] + ".equals(" + gvAttributes[i] + ")";
                } else {
                    gvfcond += "mfs[i]." + gvAttributes[i] + ".equals(" + gvAttributes[i] + ")" + " && ";
                }
            }
            gvfResult += "\t\t\tmfstemp." + gvAttributes[i] + " = " + gvAttributes[i] + ";\n";

        }
        gvFilling += gvfinner;
        gvFilling += "\t\tint i = 0;\n" +
                "\t\tboolean flag = false;\n\n" +
                "\t\twhile(i<mfsLen){\n";
        gvFilling += "\t\t\tif(" + gvfcond + "){\n";
        gvFilling += "\t\t\t\tflag = true;\n" + "\t\t\t\t" + gv0FA + "\n" +
                " \t\t\t\tbreak;\n" +
                "\t\t\t}\n" +
                "\t\t\ti++;\n" +
                "\t\t}\n" +
                "\t\tif(!flag){\n" +
                "\t\t\tMFStructure mfstemp = new MFStructure();\n";
        gvFilling += gvfResult + "\n" + "\t" + newgv0FA + "\n" + "\t\t\tmfs[mfsLen] = mfstemp;\n" +
                "\t\t\tmfsLen++;\n" +
                "\t\t\t}\n" +
                "\t\t}\n";


        String fileIO = "";
        fileIO = "\t Scanner input = new Scanner(System.in);\n";
        fileIO += "\t System.out.println(\"Enter the file name for the Output, without extension and By default path: query_output_files/ \");\n";
        fileIO += "\t String outputFile = input.nextLine();\n";
        fileIO += "\t input.close();\n";
        fileIO += "\t PrintWriter writer = new PrintWriter(\"./query_output_files/\"+outputFile.trim() + \".txt\", \"UTF-8\");\n";

        writer.write(fileIO);
        writer.write(gvFilling);
        System.out.println("Populating Grouping Attributes Done!");


        /****************  2.2) Filling the aggregates of MF Structure *********************
         *
         * Steps:
         *      1) Scan the table and match with the MF Structure
         *      2) Assumption_1 : The size of "such that" array is same as number of grouping variables
         *      3) Assumption_2 : such_that array elements are in consecutive increasing order as grouping variable
         *      4) Elements of such that is kept as it is in if condition, for simplicity :)
         *      5) For finding avg, we need count and sum in F-VECT
         * */


        // split the aggregate functions from F-VECT


        String[] suchThatCondtions = ProcessingEngine.SUCH_THAT_CONDITIONS;

        int numberOfGV = ProcessingEngine.NUM_OF_GVS;
        // Filling Aggregates
        String firstFA = "";

        // for every such that condition
        for (int i = 0; i < numberOfGV; i++) {

            String coreFA = "";
            for (String gv : gvN) {
                gv = gv.trim();
                String aggVal = i + 1 + "_" + gv;

                if (mfsDataTypes.contains("sum_" + aggVal)) {
                    coreFA += "\t\t\t\t\t" + "mfs[i].sum_" + aggVal + " += " + gv + ";\n";
                }
                if (mfsDataTypes.contains("count_" + aggVal)) {
                    coreFA += "\t\t\t\t\t" + "mfs[i].count_" + aggVal + " += " + 1 + ";\n";
                }
                if (mfsDataTypes.contains("avg_" + aggVal)) {
                    coreFA += "\t\t\t\t\t" + "try{mfs[i].avg_" + aggVal + " = " + "mfs[i].sum_" + aggVal + "/" + "mfs[i].count_" + aggVal + ";}catch(Exception e){System.out.println(e);}\n";
                }
                if (mfsDataTypes.contains("max_" + aggVal)) {
                    coreFA += "\t\t\t\t\t" + "if(mfs[i].max_" + aggVal + " < " + gv + "){" + "mfs[i].max_" + aggVal + "=" + gv + ";" + "}\n";
                }
                if (mfsDataTypes.contains("min_" + aggVal)) {
                    coreFA += "\t\t\t\t\t" + "if(mfs[i].min_" + aggVal + " > " + gv + "){" + "mfs[i].min_" + aggVal + "=" + gv + ";" + "}\n";
                }

            }
            firstFA = "\t\t/** " + i + 1 + " Loop for Gropuing attribute */\n\n" + "\t\trs.beforeFirst();\n" +
                    "\t\twhile(rs.next()){\n" +
                    "\t\t\tint i = 0;\n" +
                    "\t\t\tint quant = rs.getInt(\"quant\");\n" +
                    "\t\t\tString cust = rs.getString(\"cust\");\n" +
                    "\t\t\tString prod = rs.getString(\"prod\");\n" +
                    "\t\t\tString state = rs.getString(\"state\");\n" +
                    "\t\t\tint day = rs.getInt(\"day\");\n" +
                    "\t\t\tint month = rs.getInt(\"month\");\n" +
                    "\t\t\tint year = rs.getInt(\"year\");\n" +
                    "\t\t\twhile(i<mfsLen){\n" +
                    "\t\t\t\tif(" + suchThatCondtions[i] + "){\n" +
                    coreFA + "\n" +
                    "\t\t\t\t\t}\n" +
                    "\t\t\t\ti++;\n" +
                    "\t\t\t\t}\n" +

                    "\t\t\t}\n";

            writer.write(firstFA);
        }
       // System.out.println("Logic of Selection and Having is created");
        writer.write("\t\t/** Having Clause logic */\n\n");
        havingClause();
        selectClauseMFS();
        // Output.java created
        writer.write("\t}\n}");
        System.out.println("Output.java created, closing the connection, :)  Yeahhh!!! :) ");
        writer.close();
    }

    /**
     * Selection statements
     * Steps: 1)Iterate over Selection array and print the MF_Structure
     * 2) If logical operators is present in select
     */

    public void selectClauseMFS() throws IOException {

        // write here if single underscore then directly print that

        String[] selectClause = ProcessingEngine.SELECT_ATTRIBUTES;
        String selectOut = "";
        String innerSelect = "";
        String format = "";
        String header = "";

        for (int i = 0; i < selectClause.length - 1; i++) {

            selectClause[i] = selectClause[i].trim();
            if (selectClause[i].contains("sum_")) {
                header += '"' + selectClause[i]+'"'+ ',';
                selectClause[i] = selectClause[i].replace("sum_", "mfStructure.sum_");
                format += "| %15s ";
            } else if (selectClause[i].contains("avg_")) {
                header += '"' + selectClause[i]+'"'+ ',';
                selectClause[i] = selectClause[i].replace("avg_", "mfStructure.avg_");
                format += "| %15s ";
            } else if (selectClause[i].contains("min_")) {
                header += '"' + selectClause[i]+'"'+ ',';
                selectClause[i] = selectClause[i].replace("min_", "mfStructure.min_");
                format += "| %15s ";
            } else if (selectClause[i].contains("max_")) {
                header += '"' + selectClause[i]+'"'+ ',';
                selectClause[i] = selectClause[i].replace("max_", "mfStructure.max_");
                format += "| %15s ";
            } else if (selectClause[i].contains("count_")) {
                header += '"' + selectClause[i]+'"'+ ',';
                selectClause[i] = selectClause[i].replace("count_", "mfStructure.count_");
                format += "| %15s ";
            } else {
                header += '"' + selectClause[i] +'"'+  ',';
                innerSelect += "mfStructure.";
                format += "| %-15s ";
            }
            innerSelect += selectClause[i] + ",";
        }

        // for last element
        if (selectClause[selectClause.length - 1].contains("sum_")) {
            header += '"' + selectClause[selectClause.length-1]  + '"';
            selectClause[selectClause.length - 1] = selectClause[selectClause.length - 1].replace("sum_", "mfStructure.sum_");
            format += "| %15s |";
        } else if (selectClause[selectClause.length - 1].contains("avg_")) {
            header += '"' + selectClause[selectClause.length-1] + '"';
            selectClause[selectClause.length - 1] = selectClause[selectClause.length - 1].replace("avg_", "mfStructure.avg_");
            format += "| %15s |";
        } else if (selectClause[selectClause.length - 1].contains("min_")) {
            header += '"' + selectClause[selectClause.length-1] + '"';
            selectClause[selectClause.length - 1] = selectClause[selectClause.length - 1].replace("min_", "mfStructure.min_");
            format += "| %15s |";
        } else if (selectClause[selectClause.length - 1].contains("max_")) {
            header += '"' + selectClause[selectClause.length-1] + '"';
            selectClause[selectClause.length - 1] = selectClause[selectClause.length - 1].replace("max_", "mfStructure.max_");
            format += "| %15s |";
        } else if (selectClause[selectClause.length - 1].contains("count_")) {
            header += '"' + selectClause[selectClause.length-1] + '"';
            selectClause[selectClause.length - 1] = selectClause[selectClause.length - 1].replace("count_", "mfStructure.count_");
            format += "| %15s |";
        } else {
            header += '"' + selectClause[selectClause.length-1] + '"';
            innerSelect += "mfStructure.";
            format += "| %-15s |";
        }



        innerSelect += selectClause[selectClause.length - 1];

        selectOut += "\t\t" + "String format = "+'"'+ format + "\\n\";" + '\n';
        selectOut += "\t\t" + "System.out.format(format," + header + ");\n" + "\n";
        selectOut += "\t\t" + "writer.format(format," + header + ");\n" + "\n";
        selectOut += "\t\t" + "System.out.println('\\n');" + "\n";
        selectOut += "\t\t" + "writer.println('\\n');" + "\n";
        selectOut += "\t\t" + "for (MFStructure mfStructure:mfsList){\n";
        selectOut += "\t\t\t" + "try{System.out.format(format," + innerSelect + ");writer.format(format," + innerSelect + "); }\n" + "\t";
        selectOut += "\t\t    catch(Exception e) {\n" +
                "\t\t        continue;\n" +
                "            \t}\n            }        \n";
        writer.write("\t\t/** Loop for Select Clause for output the result */\n\n");
        writer.write(selectOut);
        writer.write( "\t\twriter.close();\n");
    }

    /**
     * Having Conditions
     * Steps: 1)Iterate over Selection array and print the MF_Structure
     */

    public void havingClause() throws Exception {

        String[] havingClauses = ProcessingEngine.HAVING_CONDITIONS;
        String havingClauseOut = "";
        String havingClauseFirst = "";

        if (havingClauses[0].equals("")) {
            havingClauseOut = "\t\t\t/**  No having clause*/\n";
            havingClauseFirst = "\t\t\t\t\tmfsList.add(mfs[i]);\n";
        } else {
            for (int i = 0; i < havingClauses.length; i++) {
                havingClauseFirst = "\t\t\t\t\tif(" + havingClauses[i] + "){\n" + "\t\t\t\t\tmfsList.add(mfs[i]);\n" + "\t\t\t\t\t}\n";
            }
        }
        havingClauseOut += "\t\t\tfor(int i= 0;i<mfsLen;i++){\n" +
                "\t\t\t\tif(mfs[i] != null){\n" +
                havingClauseFirst +
                "\t\t\t\t}\n" +
                "\t\t\t}\n";


        writer.write(havingClauseOut);
    }
}
