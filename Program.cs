using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using RocksDbSharp;
using RocksDB;
using Newtonsoft.Json;
using System.Data.SQLite;
using Microsoft.Data.Sqlite;
using System.Data.SqlClient;
using System.Diagnostics;

namespace RocksDB
{


    class Program
    {
        static void Main(string[] args)
        {
            //SQLite sQ = new SQLite();
            //sQ.Execute();

            //MicrosoftSQLite micro = new MicrosoftSQLite();
            //micro.Execute("AllCountries.tsv");
        }

    }
    // The RocksDB class is used for the creation of RocksDB databases
    public class RocksDB
    {



        public void LoadFile(string fileName, string folderName)
        {
            string tempPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string[] pathSplit = tempPath.Split('\\');
            string newpath = "";
            for (int i = 0; i < pathSplit.Length - 3; i++)
            {
                newpath += pathSplit[i] + "\\";
            }

            string inputFile = newpath + '\\' + fileName;
            string path = newpath + '\\' + folderName + '\\';

            using (var reader = new StreamReader(inputFile, System.Text.Encoding.UTF8))
            {
                var options = new DbOptions().SetCreateIfMissing(true).SetCompression(CompressionTypeEnum.rocksdb_snappy_compression);
                using (var db = RocksDb.Open(options, path))
                {
                    var line = reader.ReadLine(); // throw away the header line

                    while (!reader.EndOfStream)
                    {
                        line = reader.ReadLine();
                        var values = line.Split(',');

                        string key = "";
                        string value = "";

                        for (int i = 1; i < values.Length; i++)
                        {
                            // Iterates Through Each Line and Assigns the Values and Keys
                            key = values[i];
                            value = values[0];

                            if (key.Equals(""))
                            {
                                continue;
                            }
                            else
                            {
                                // Used to Make the Key and Values Case-Insensitive Depending on What You Want
                                key = key.ToUpper();
                                value = value.ToUpper();
                                // key = key.ToLower();
                                // value = value.ToLower();

                                // Puts the Key and Correspoonding Value into RocksDB
                                db.Put(key, value);
                            }

                        }


                    }
                    // Uncomment This Section and Comment out While Loop Above for The Original Key Value System

                    //while (!reader.EndOfStream)
                    //{
                    //    line = reader.ReadLine();
                    //    var values = line.Split(',');

                    //    string key = values[0];
                    //    string value = "";

                    //    for (int i = 1; i < values.Length; i++)
                    //    {
                    //        value = values[i] + " ";
                    //    }

                    //    if (key.Equals(""))
                    //    {
                    //        continue;
                    //    }
                    //    else
                    //    {
                    //        db.Put(key, value);
                    //    }

                    //}
                }
            }
        }

        public string Lookup(string MAK, string folderName)
        {
            string tempPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string[] pathSplit = tempPath.Split('\\');
            string newpath = "";
            for (int i = 0; i < pathSplit.Length - 3; i++)
            {
                newpath += pathSplit[i] + "\\";
            }
            string path = newpath + '\\' + folderName + '\\';
            var options = new DbOptions();
            using (var db = RocksDb.Open(options, path))
            {
                return db.Get(MAK);
            }
        }
    }

    // The SQL class holds everything needed to create and fill a SQL table from a .csv file or .tsv file
    public class SQL
    {
        public void Execute(string server, string dataBase, string tableName, string fileName)
        {
            // Opens the connection into the SQL table that's wanted to write to
            SqlConnection connect = new SqlConnection("SERVER=" + server + ";database=" + dataBase + ";Integrated Security=SSPI;MultipleActiveResultSets = True");
            string tempPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string[] pathSplit = tempPath.Split('\\');
            string path = "";
            for (int i = 0; i < pathSplit.Length - 3; i++)
            {
                path += pathSplit[i] + "\\";
            }
            connect.Open();
            string file = path + fileName;
            // The StreamReader opens the file above to start reading from it
            // The UTF8 encoding is necessary in this specific case due to the non-English characters that are within the file
            StreamReader sr = new StreamReader(file, System.Text.Encoding.UTF8);
            string query = "";
            string value = "";
            string[] split;
            sr.ReadLine();
            while ((value = sr.ReadLine()) != null)
            {
                // Splits each line of the file based on (\t) for .tsv files and (,) for .csv files
                if(file.EndsWith(".csv"))
                {
                    split = value.Split(',');
                }
                else
                {
                    split = value.Split('\t');
                }
                // Stores the key as the first element in the line
                string countryCode = split[0];
                for (int i = 1; i < split.Length; i++)
                {
                    // Stores the value by every other element in the line
                    string countryName = split[i];
                    countryName = countryName.Replace("'", "$");
                    if (countryName != "" && countryCode != "")
                    {
                        query = "INSERT INTO [" + dataBase + "].[dbo].[" + tableName + "] VALUES ('" + countryName + "','" + countryCode + "');\n";
                        SqlCommand fillData = new SqlCommand(query, connect);
                        fillData.ExecuteNonQuery();
                    }

                }

            }

            connect.Close();
     
        }
        
    }

    // The SQLite Class holds everything needed for using the SQLite extension/package
    public class SQLite
    {
        // Execute() class reads from a .tsv or .csv file and separates the contents into a key/value database
        public void Execute(string fileName, string tableName, string sqliteTablePath)
        {
            string tempPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string[] pathSplit = tempPath.Split('\\');
            string path = "";
            for (int i = 0; i < pathSplit.Length - 3; i++)
            {
                path += pathSplit[i] + "\\";
            }
            SQLiteConnection.CreateFile(tableName + ".sqlite");

            // This connect line creates the SQLite database that will be then added to later.
            SQLiteConnection connect = new SQLiteConnection("Data Source=" + sqliteTablePath + '\\' + tableName + ".sqlite;Version=3;UseUTF16Encoding=True;");
            connect.Open();

            // This string represents a SQL command that will create a table within the database that was created above
            string sql = "CREATE TABLE " + tableName + " (CountryName VARCHAR(256), CountryCode VARCHAR(256))";

            // This command line opens the connection so the newly created datatable can be used
            SQLiteCommand command = new SQLiteCommand(sql, connect);

            try
            {
                // ExecuteNonQuery() is used to the sql string and create the table within the database
                command.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                Console.Write(ex.Message);
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();
                Console.Write(line.ToString());
                //return;
            }
            string file = path + fileName;
            // This StreamReader intiates a reading from a specified file
            // The UTF8 encoding is used because the file contains characters from different languages so an encoding is necessary
            StreamReader sr = new StreamReader(file, System.Text.Encoding.UTF8);
            string query = "";
            string value = "";
            string[] split;
            sr.ReadLine();
            while ((value = sr.ReadLine()) != null)
            {
                // Splits each line from the StreamReader based upon (\t) for .tsv files and (,) for .csv files
                if(file.EndsWith(".tsv"))
                {
                    split = value.Split('\t');
                }
                else
                {
                    split = value.Split(',');
                }
                // Declares the key to be the first element in the line
                string countryCode = split[0];
                for (int i = 1; i < split.Length; i++)
                {
                    // Declares the values to be everything else in the line
                    string countryName = split[i];
                    countryName = countryName.Replace("'", "$");
                    if (countryName != "" && countryCode != "")
                    {
                        char first = countryName[0];
                        if(!char.IsDigit(first))
                        {
                            // Uses the declared key and values and puts them into the SQLite database
                            query = "INSERT INTO " + tableName + "(CountryName, CountryCode) VALUES ('" + countryName.ToUpper() + "','" + countryCode.ToUpper() + "');\n";
                            SQLiteCommand fillData = new SQLiteCommand(query, connect);
                            fillData.ExecuteNonQuery();
                        }
                    }

                }

            }

            connect.Close();
            sr.Close();
        }
        
    }
    public class MicrosoftSQLite
    {
        public void Execute(string fileName, string tableName, string sqliteTablePath)
        {
            string tempPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string[] pathSplit = tempPath.Split('\\');
            string path = "";
            for(int i = 0; i < pathSplit.Length - 3; i++)
            {
                path += pathSplit[i] + "\\";
            }
            SqliteConnection connect = new SqliteConnection("Data Source=" + sqliteTablePath + '\\'+ tableName + ".sqlite");
            connect.Open();

            string sql = "CREATE TABLE " + tableName + " (CountryName VARCHAR(256), CountryCode VARCHAR(256))";

            SqliteCommand command = new SqliteCommand(sql, connect);

            try
            {
                // ExecuteNonQuery() is used to the sql string and create the table within the database
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.Write(ex.Message);
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();
                Console.Write(line.ToString());
                //return;
            }

            string file = path + fileName;
            // This StreamReader intiates a reading from a specified file
            // The UTF8 encoding is used because the file contains characters from different languages so an encoding is necessary
            StreamReader sr = new StreamReader(file, System.Text.Encoding.UTF8);
            string query = "";
            string value = "";
            string[] split;
            sr.ReadLine();
            while ((value = sr.ReadLine()) != null)
            {
                // Splits each line from the StreamReader based upon (\t) for .tsv files and (,) for .csv files
                if (file.EndsWith(".tsv"))
                {
                    split = value.Split('\t');
                }
                else
                {
                    split = value.Split(',');
                }
                // Declares the key to be the first element in the line
                string countryCode = split[0];
                for (int i = 1; i < split.Length; i++)
                {
                    // Declares the values to be everything else in the line
                    string countryName = split[i];
                    countryName = countryName.Replace("'", "$");
                    if (countryName != "" && countryCode != "")
                    {
                        char first = countryName[0];
                        if (!char.IsDigit(first))
                        {
                            // Uses the declared key and values and puts them into the SQLite database
                            query = "INSERT INTO CountryCodeNames(CountryName, CountryCode) VALUES ('" + countryName.ToUpper() + "','" + countryCode.ToUpper() + "');\n";
                            SqliteCommand fillData = new SqliteCommand(query, connect);
                            fillData.ExecuteNonQuery();
                        }
                    }

                }

            }

            connect.Close();
            sr.Close();
        }
    }
}

