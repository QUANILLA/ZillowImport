using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using static System.Console;
using static System.Text.RegularExpressions.Regex;

namespace ZillowImport
{
    class Program
    {

        static readonly Dictionary<string, string> SqlTypeMapping = new Dictionary<string, string>
        {
            ["String"] = "VARCHAR(400)",
            ["DateTime"] = "DATE"
        };
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                WriteLine("usage: ZillowImport <server> <database> <file>");
                return;
            }

            string server = args[0];
            string database = args[1];
            string file = args[2];

            string connectionString = $"Server={server};Database={database};Trusted_Connection=True;";
            WriteLine($"file = {file}, server = {server}, database = {database}");
            using (ZillowImporter importer = new ZillowImporter(new CsvReader(new StreamReader(file), true)))
            {
                string tableName = $"[{Path.GetFileNameWithoutExtension(file)}]";
                DataTable dt = importer.DataTable;
                string columnsList = string.Join("," + Environment.NewLine, dt.Columns.Cast<DataColumn>().Select(x => $"    [{x.ColumnName}] {SqlTypeMapping[x.DataType.Name]} NOT NULL"));
                string createTableScript = $@"
IF (OBJECT_ID('{tableName}') IS NOT NULL)
BEGIN
    DROP TABLE {tableName}
END

CREATE TABLE {tableName}(
	{columnsList}
) ON [PRIMARY]
";
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(createTableScript, conn))
                    {
                        
                        cmd.CommandType = CommandType.Text;
                        conn.Open();
                        WriteLine($"executing {createTableScript}");
                        cmd.ExecuteNonQuery();
                    }
                    WriteLine($"executing bulk copy...");
                    DateTime start = DateTime.Now;
                    using (var bc = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
                    {
                        bc.DestinationTableName = tableName;
                        bc.BatchSize = 1000;
                        bc.BulkCopyTimeout = 3000;
                        bc.WriteToServer(importer);
                    }
                    TimeSpan elapsed = DateTime.Now - start;
                    WriteLine($"copied {importer.RowsRead} rows in {elapsed.ToString()}");
                }
            }
        }

        

        class ZillowImporter : IDataReader
        {
            public ZillowImporter(CsvReader csv)
            {
                csvReader = csv;
                var headersList = csvReader.GetFieldHeaders().ToList();
                WriteLine("headers:");
                WriteLine(string.Join(", ", headersList));
                var datesAndIndexes =
                    (
                    from i in Enumerable.Range(0, headersList.Count)
                    let header = headersList[i]
                    where IsMatch(header, @"\d{4}-\d{2}")
                    let year = header.Substring(0, 4)
                    let month = header.Substring(5, 2)
                    select new { Index = i, Date = DateTime.Parse($"{month}/1/{year}") }
                    ).ToList();
                dates = datesAndIndexes.Select(x => x.Date).ToArray();
                var firstDate = datesAndIndexes.FirstOrDefault();
                dateColumnOrdinal = firstDate != null ? ((int?)(headersList.Count() - 1)) : null;
                fieldCount = firstDate != null ? firstDate.Index + 1 : headersList.Count;
                headers =
                    (
                    from i in Enumerable.Range(0, fieldCount)
                    select i != firstDate?.Index ? headersList[i] : "Date"
                    ).ToArray(); 
                ordinalMapping =
                    (
                    from i in Enumerable.Range(0, fieldCount)
                    select new { Ordinal = i, Field = headersList[i] }
                    ).ToDictionary(x => x.Field, x => x.Ordinal);

                dateCol = firstDate?.Index;
                WriteLine($"first date header = {firstDate?.Index}");
                enumerable = GetRows();
                enumerator = enumerable.GetEnumerator();
                dataTable = new DataTable();
                foreach (int i in Enumerable.Range(0, headers.Count()))
                {
                    Type type = i != dateColumnOrdinal ? typeof(string) : typeof(DateTime);
                    string header = headers[i];
                    dataTable.Columns.Add(header, type);
                }
            }

            public string GetCreateTableScript()
            {
                string result = null;

                return result;
            }

            private IEnumerable<List<object>> GetRows()
            {
                while (csvReader.ReadNextRecord())
                {
                    var rowValues =
                        (from col in Enumerable.Range(0, fieldCount)
                         select (object)csvReader[col]
                        ).ToList();
                    if (dates.Length > 0)
                    {
                        rowValues.Add(null);
                        foreach (var date in dates)
                        {
                            rowValues[dateCol.Value] = date.Date.ToShortDateString();
                            yield return rowValues;
                        }
                    }
                    else
                    {
                        yield return rowValues;
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
                return ordinalMapping[name];
            }

            public object GetValue(int i)
            {
                if (rowValues != null)
                {
                    return rowValues[i];
                }
                else
                {
                    throw new InvalidOperationException("No more rows");
                }
            }

            public bool Read()
            {
                RowsRead++;
                var result = enumerator.MoveNext();
                rowValues = result ? enumerator.Current : null;
                if (RowsRead % 1000000 == 0)
                {
                    WriteLine($"{RowsRead} rows...");
                }
                return result;
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

            public DataTable DataTable
            {
                get
                {
                    return dataTable;
                }
            }

            public int RowsRead { get; private set; }

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
            private readonly int? dateCol;
            private readonly DateTime[] dates;
            private readonly IEnumerable<List<object>> enumerable;
            private readonly IEnumerator<List<object>> enumerator;
            private List<object> rowValues;
            private readonly string[] headers;
            private readonly int? dateColumnOrdinal;
            private readonly DataTable dataTable;

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        // TODO: dispose managed state (managed objects).
                        try
                        {
                            csvReader.Dispose();
                        }
                        catch
                        {
                        }
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
    }
}
