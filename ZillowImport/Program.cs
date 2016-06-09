using System;
using System.IO;
using LumenWorks.Framework.IO.Csv;
using static System.Console;

namespace ZillowImport
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                WriteLine("usage: ZillowImport <file> <server> <database>");
                return;
            }

            string file = args[0];
            string server = args[1];
            string database = args[2];

            WriteLine($"file = {file}, server = {server}, database = {database}");
            using (CsvReader csv = new CsvReader(new StreamReader(file), true))
            {
                int fieldCount = csv.FieldCount;

                string[] headers = csv.GetFieldHeaders();
                WriteLine("headers:");
                WriteLine(string.Join(", ", headers));
                //while (csv.ReadNextRecord())
                //{
                //    for (int i = 0; i < fieldCount; i++)
                //        Console.Write(string.Format("{0} = {1};",
                //                      headers[i], csv[i]));
                //    Console.WriteLine();
                //}
            }
        }
    }
}
