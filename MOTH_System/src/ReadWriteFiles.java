package MOTH_System;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ReadWriteFiles {


	private static final int ITERATIONS = 1;
	private static final double MEG = (Math.pow(1024, 2));
	private static final int RECORD_COUNT = 100;//4000000;
	private static final String RECORD = "Help I am trapped in a fortune cookie factory\n";
	private static final int RECSIZE = RECORD.getBytes().length;

	public static void main(String[] args) throws Exception{ 

		String sql="create table t1 as select L_ORDERKEY, l_discount from t2 where l_discount between 0.01 and 0.02";
		get_table_name(sql);
	}



	public static  List<List<String>>  read_to_ListofList( ) throws IOException 
	{
		File inifile = new File ("/home/hadoop/NetBeansProjects/UDF/SharedHive.sql");
		//System.out.println(file_name);
		Scanner read = new Scanner (inifile).useDelimiter(" ");

		List<List<String>> Allquerylist = new ArrayList<List<String>>();
		String line="";
		while (read.hasNextLine())
		{
			List<String> querylist = new ArrayList<String>();

			line=read.nextLine();
			if(line.equals("#"))
			{
				Allquerylist.add(querylist);
				querylist.clear();
			}
			else
				querylist.add(line);
		}


		System.out.println(Allquerylist.get(0).get(Allquerylist.get(0).size()-1));
		//String result[][]=new String
		return Allquerylist;
	}

	public static  List<String> read_to_List( ) throws IOException 
	{
		File inifile = new File ("/home/hadoop/NetBeansProjects/UDF/Concurrent.sql");
		//System.out.println(file_name);
		Scanner read = new Scanner (inifile).useDelimiter(" ");

		List<String> querylist = new ArrayList<String>();
		String line="";
		while (read.hasNextLine())
		{
			line=read.nextLine();
			querylist.add(line);
			String table_name=get_table_name(line);
		}


		//System.out.println(Allquerylist.get(0).get(Allquerylist.get(0).size()-1));
		//String result[][]=new String
		return querylist;
	}

	public static String  get_table_name(String sql)
	{
		//System.out.println(sql);
		int indexoftable= sql.indexOf("table"); 
		int indexofas=sql.indexOf("as");
		int table_name_postion=indexoftable+6;
		String table_name=sql.substring(table_name_postion, indexofas).trim();


		System.out.println("table name..."+table_name);
		return table_name;

	}


	public static void writeBuffered(String file_name, List<String> records, int bufSize) throws IOException {
		try {
			File currentDir = new File("");
			String full_file_name=currentDir.getAbsolutePath()+"/TempQueryResults/"+file_name;
			System.out.println(full_file_name);
			FileWriter writer = new FileWriter(full_file_name);
			BufferedWriter bufferedWriter = new BufferedWriter(writer, bufSize);


			write(records, bufferedWriter);
		} finally {
			// comment this out if you want to inspect the files afterward
			//file.delete();
		}
	}

	public static void write(List<String> records, Writer writer) throws IOException {
		long start = System.currentTimeMillis();
		for (String record: records) {
			writer.write(record);
		}
		writer.flush();
		writer.close();
		long end = System.currentTimeMillis();
		//System.out.println((end - start) / 1000f + " seconds");
		//System.out.println("File created");
	}
	public static void write_single_query(String sql) throws IOException
	{


		FileWriter fstream;
		BufferedWriter out;
		BufferedWriter outtempsql;
		File sqlfile;

		fstream = new FileWriter ("one_query.sql",false);
		outtempsql = new BufferedWriter (fstream);
		outtempsql.write(sql);
		// outtempsql.write("\n");
		outtempsql.flush();
		outtempsql.close();

		//must open for parser
		sqlfile = new File ("one_query.sql");
		//System.out.println("File exists :  " + sqlfile);
		if (!sqlfile.exists()){
			System.out.println("File not exists :  " + sqlfile);
			return;
		}
	}//  end fuction write_single_query 

}// end of class