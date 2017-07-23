﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;

namespace ConsoleApplication2
{
    class Program
    {
        private static DataTable GetDataTabletFromCsvFile(string csvFilePath)
        {
            var csvData = new DataTable();
            try
            {
                using (var csvReader = new TextFieldParser(csvFilePath))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true; //Are fields in our data enclosed in quote marks?
                    var colFields = csvReader.ReadFields();
                    foreach (var column in colFields)
                    {
                        var datecolumn = new DataColumn(column) {AllowDBNull = true};
                        csvData.Columns.Add(datecolumn);
                    }

                    //This next loop iterates over our csvData object until it hits the end of the file.
                    while (!csvReader.EndOfData)
                    {
                        object[] fieldData = csvReader.ReadFields();
                        //Firstly, check that our fields are not actually null, in other words, that our csvData is not empty.
                        Debug.Assert(fieldData != null, "fieldData != null");
                        //Setting all empty values in the field data to the null type.
                        for (var i = 0; i < fieldData.Length; i++)
                        {
                            if ((string) fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                return null;
            }
            return csvData;
        }


        //This method will drop, then recreate the tables for our CSV files, using the headers in each file's top row.
        private string[] getSqlTableHeaders(string csvFilePath)
        {
            string[] sqlTableHeaders = File.ReadLines(csvFilePath).First().Split(',');
            return sqlTableHeaders;
        }

        //This method will generate the CREATE TABLE SQL statement for each array of sqlTableHeaders taken from a single CSV file.
        //It can be iterated to create multiple CREATE TABLE transactions for each CSV file.
        private string initialiseTablesSqlString(string sqlTableName, string[] sqlTableHeaders)
        {
            string createTablesString = String.Format("IF OBJECT_ID('dbo.{0}', 'U') IS NOT NULL DROP TABLE dbo.{0};\nCREATE TABLE {0} (ID INTEGER,\n", sqlTableName);
            foreach (string column in sqlTableHeaders)
            {
                if (column == "Date") { createTablesString += "Date DATE,\n"; }
                else if (column == "StratName") { createTablesString += "StratName VARCHAR(255),\n"; }
                //We expect the below case to be the last column in the only table this appears in.
                else if (column == "Region") { createTablesString += "Region VARCHAR(255)"; }
                //The below case should run and exit the loop if the column is the last one, and is not the Region column header, in the array of headers.
                else if (column == sqlTableHeaders.Last()) { createTablesString += String.Format("{0} FLOAT", column); }
                //When all the other cases are false, which should be true for all strategy columns, we create a FLOAT column.
                else createTablesString += String.Format("{0} FLOAT,\n", column);
            }
            createTablesString += ");";

            return createTablesString;
        }


        private static void InsertDataIntoSqlServerUsingSqlBulkCopy(DataTable csvFileData, string tableName)
        {
            //The connection string below is for an already initialised database. 
            //TODO: Need to find a way to run temporary initialised databases, possibly.
            using (var dbConnection =
                new SqlConnection("Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=tempdb;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
            {
                dbConnection.Open();

                //TODO: there needs to be a way to clear existing tables, and reinitialise them if possible.
                //The below copies the csvFileData DataTable passed as input, to the specified SQL Server database table.
                using (var s = new SqlBulkCopy(dbConnection))
                {
                    s.DestinationTableName = tableName;
                    foreach (var column in csvFileData.Columns)
                    {
                        s.ColumnMappings.Add(column.ToString(), column.ToString());
                        s.WriteToServer(csvFileData);
                    }
                }
            }
        }


        private static void Main(string[] args)
        {
            const string excelCapitalFilePath = @"D:\Documents\GSA Developer test\capital.csv";
            const string excelPnlFilePath = @"D:\Documents\GSA Developer test\pnl.csv";
            const string excelPropertiesFilePath = @"D:\Documents\GSA Developer test\properties.csv";

            string[] filePathStrings = {excelCapitalFilePath, excelPnlFilePath, excelPropertiesFilePath};
            var sqlTableNames = new Dictionary<string, string>()
            {
                {excelCapitalFilePath, "capital"},
                {excelPnlFilePath, "pnl" },
                {excelPropertiesFilePath, "properties"}

            };

            foreach (var filePathString in filePathStrings)
            {
                Console.WriteLine("Converting file {0} to DataTable...", filePathString);
                var dataSet = GetDataTabletFromCsvFile(filePathString);
                InsertDataIntoSqlServerUsingSqlBulkCopy(dataSet, sqlTableNames[filePathString]);
                Console.WriteLine("Completed writing CSV data to SQL Server Database.");
            }
            Console.ReadKey();
        }
    }
}