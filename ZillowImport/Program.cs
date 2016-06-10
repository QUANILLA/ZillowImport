using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Data;
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
            count = Read(file, count);
        }

        class ZillowImporter : IDataReader
        {
            public ZillowImporter(CsvReader csv)
            {
                this.csvReader = csv;

                var headers = csvReader.GetFieldHeaders().ToList();
                WriteLine("headers:");
                WriteLine(string.Join(", ", headers));
                var dates =
                    (
                    from i in Enumerable.Range(0, headers.Count)
                    let header = headers[i]
                    where IsMatch(header, @"\d{4}-\d{2}")
                    let year = header.Substring(0, 4)
                    let month = header.Substring(5, 2)
                    select new { Index = i, Date = DateTime.Parse($"{month}/1/{year}") }
                    ).ToList();
                var firstDate = dates.FirstOrDefault();
                if (firstDate != null)
                {
                    headers.Add("Date");
                }
                fieldCount = (firstDate?.Index + 1) ?? headers.Count;
                ordinalMapping =
                    (
                    from i in Enumerable.Range(0, fieldCount)
                    select new { Ordinal = i, Field = headers[i] }
                    ).ToDictionary(x => x.Field, x => x.Ordinal);

                var dateCol = firstDate?.Index;
                WriteLine($"first date header = {firstDate.Index}");
                while (csvReader.ReadNextRecord())
                {
                    var rowValues =
                        (from col in Enumerable.Range(0, fieldCount)
                         select csvReader[col]
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
                }
            }

            public int FieldCount
            {
                get
                {
                    return fieldCount;
                }
            }
            public int GetOrdinal(string name)
            {
                throw new NotImplementedException();
            }

            public object GetValue(int i)
            {
                throw new NotImplementedException();
            }

            public bool Read()
            {
                throw new NotImplementedException();
            }

            #region Unused by SqlBulkCopy
            public object this[string name]
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public object this[int i]
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public int Depth
            {
                get
                {
                    throw new NotImplementedException();
                }
            }


            public bool IsClosed
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public int RecordsAffected
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public void Close()
            {
                throw new NotImplementedException();
            }

            public bool GetBoolean(int i)
            {
                throw new NotImplementedException();
            }

            public byte GetByte(int i)
            {
                throw new NotImplementedException();
            }

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public char GetChar(int i)
            {
                throw new NotImplementedException();
            }

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i)
            {
                throw new NotImplementedException();
            }

            public string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i)
            {
                throw new NotImplementedException();
            }

            public decimal GetDecimal(int i)
            {
                throw new NotImplementedException();
            }

            public double GetDouble(int i)
            {
                throw new NotImplementedException();
            }

            public Type GetFieldType(int i)
            {
                throw new NotImplementedException();
            }

            public float GetFloat(int i)
            {
                throw new NotImplementedException();
            }

            public Guid GetGuid(int i)
            {
                throw new NotImplementedException();
            }

            public short GetInt16(int i)
            {
                throw new NotImplementedException();
            }

            public int GetInt32(int i)
            {
                throw new NotImplementedException();
            }

            public long GetInt64(int i)
            {
                throw new NotImplementedException();
            }

            public string GetName(int i)
            {
                throw new NotImplementedException();
            }

            public DataTable GetSchemaTable()
            {
                throw new NotImplementedException();
            }

            public string GetString(int i)
            {
                throw new NotImplementedException();
            }

            public int GetValues(object[] values)
            {
                throw new NotImplementedException();
            }

            public bool IsDBNull(int i)
            {
                throw new NotImplementedException();
            }

            public bool NextResult()
            {
                throw new NotImplementedException();
            }


            #endregion

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls
            private readonly CsvReader csvReader;
            private readonly int fieldCount;
            private readonly Dictionary<string, int> ordinalMapping;

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        // TODO: dispose managed state (managed objects).
                    }

                    // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                    // TODO: set large fields to null.

                    disposedValue = true;
                }
            }

            // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
            // ~ZillowImporter() {
            //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            //   Dispose(false);
            // }

            // This code added to correctly implement the disposable pattern.
            public void Dispose()
            {
                // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
                Dispose(true);
                // TODO: uncomment the following line if the finalizer is overridden above.
                // GC.SuppressFinalize(this);
            }
            #endregion

        }

        private static int Read(string file, int count)
        {
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

            return count;
        }

        private static void PrintRow(List<string> rowValues)
        {
            WriteLine(string.Join(",", rowValues));
        }
    }
}
