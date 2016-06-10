using LumenWorks.Framework.IO.Csv;
using System;
using System.IO;
using System.Linq;
using static System.Console;
using static System.Text.RegularExpressions.Regex;

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
            int count = 0;
            using (CsvReader csv = new CsvReader(new StreamReader(file), true))
            {
                int fieldCount = csv.FieldCount;

                string[] headers = csv.GetFieldHeaders();
                WriteLine("headers:");
                WriteLine(string.Join(", ", headers));
                var dates =
                    (
                    from i in Enumerable.Range(0, headers.Length)
                    let header = headers[i]
                    where IsMatch(header, @"\d{4}-\d{2}")
                    let year = header.Substring(0, 4)
                    let month = header.Substring(5, 2)
                    select new { Index = i, Date = DateTime.Parse($"{month}/1/{year}") }
                    ).ToList();
                var firstDate = dates.FirstOrDefault();
                var actualHeaderCount = firstDate?.Index ?? headers.Length;
                var dateCol = firstDate?.Index;
                WriteLine($"first date header = {firstDate.Index}");
                while (csv.ReadNextRecord())
                {
                    var rowValues =
                        (from col in Enumerable.Range(0, actualHeaderCount)
                         select csv[col]
                        ).ToList();
                    if (dates.Count > 0)
                    {
                        rowValues.Add(null);
                        foreach (var date in dates)
                        {
                            rowValues[dateCol.Value] = date.Date.ToShortDateString();
                            PrintRow(rowValues);
                        }
                    }
                    else
                    {
                        PrintRow(rowValues);
                    }
                    count++;
                    if (count > 1)
                        break;
                }
            }
        }

        private static void PrintRow(System.Collections.Generic.List<string> rowValues)
        {
            WriteLine(string.Join(",", rowValues));
        }
    }
}
