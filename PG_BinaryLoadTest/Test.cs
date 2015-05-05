using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.IO;
using Npgsql;
using System.Collections;
using System.Diagnostics;

using DRX.Database.PostgreSQL;
using Nini.Config;
using PostgreSQL_Load;

namespace PG_BinaryLoad_Test
{
    /// <summary>
    /// PostgreSQL Binary Load Copy Test Application
    /// Pre-requisites:
    /// - PostgreSQL >=9 & a user with administration privileges
    /// - .NET >=2.0
    /// </summary>
    class Test
    {
        private Random rand;
        private PGTestData testData;

        static void Main(string[] args)
        {
            new Test().Run();
        }

        public Test()
        {
            rand = new Random(DateTime.Now.Millisecond);
            this.testData = new PGTestData();
        }

        /// <summary>
        ///  1. Read configuration from 
        ///  2. Connect to PostgreSQL 9+ DB
        ///  3. Create example table if it does not exist based on TableName & ColumnMap
        ///  4. Generate some example in data based on settings defined in ColumnMap
        ///  5. Generate PostgreSQL binary data
        ///  6. Copy binary data in to PostgreSQL
        /// </summary>
        public void Run()
        {
            String tempFile = null;
            Stream ms = null;
            try
            {
               // Log("json=" + new PGTestData().RandomJsonGet());
                IniConfigSource source;
                String cfgFile = System.AppDomain.CurrentDomain.FriendlyName + ".ini";
                if (File.Exists(cfgFile))
                {
                    source = new IniConfigSource(cfgFile);
                } else {
                    source = new IniConfigSource();
                }
                
                if (source.Configs["Configuration"] == null)
                {
                    source.AddConfig("Configuration");
                }
                var config = source.Configs["Configuration"];

                Log("cfgFile=" + cfgFile);
                String cStr = config.Get("ConnectionString", "Server=localhost;Port=5432;Database=postgres;User Id=postgres;Password=postgres"); // DB connection string
                String table = config.Get("TableName","BinaryCopyInTest"); // Table name
                int rows = int.Parse("" + config.Get("RowCount","1000000")); // Number of rows
                String columnMap = config.Get("ColumnMap", "column3=varchar(64),column4=text(128),column5=json"); // Test data column definitions
                bool streamData = bool.Parse("" + config.Get("StreamData", "True")); // Stream the data instead to MemoryStream of generating a DataTable first
                tempFile = config.Get("TemporaryFile", ""); // Use a temporary file to hold stream data instead of MemoryStream. This should be SSD storage

                Log("ConnectionString: " + cStr);
                Log("TableName: " + table);
                Log("RowCount: " + rows);
                Log("ColumnMap: " + columnMap);
                Log("StreamData: " + streamData);
                Log("TemporaryFile: " + (String.IsNullOrEmpty(tempFile) ? "N/A (MemoryStream)" : tempFile));
                
                Log("Press Enter to continue...");
                Console.ReadLine();
                
                NpgsqlConnection con = DbConnect(cStr);
                // Get a stream to write to write temporary data to i.e MemoryStream or FileStream
                ms = TempStreamGet(tempFile);

                DateTime dtStart = DateTime.Now;
                DataTable exampleData = new DataTable(table);
                List<String> serialColumns = new List<string>();
                double timeMs = 0;
                if (!streamData)
                {
                    // Write test data to a DataTable and then create a binary Stream from it
                    Log("Generate " + rows + " rows of example data for table: " + table);
                    exampleData = DataGenRandomToTable(exampleData, rows, columnMap);
                    // Get a list of serial (auto-increment) columns, as these need to be excluded from the SQL copy command
                    serialColumns = SerialColumnNamesGet(exampleData.Columns);

                    Log("Pre-computed TestData DataTable -> Stream");
                    DateTime dtBefore = DateTime.Now;
                    PG_BinaryLoad.FromDataTable(ms, exampleData);
                    timeMs = (DateTime.Now - dtBefore).TotalMilliseconds;
                    Log("Complete. Size: " + ms.Length + " bytes. TimeTaken: " + timeMs + "ms");
                }
                else
                {
                    // Parse the ColumnMap and create these columns in the DataTable
                    ColumnMapsParseSet(exampleData, columnMap);
                    serialColumns = SerialColumnNamesGet(exampleData.Columns);
                    Log("Generate TestData direct to Stream");
                    DateTime dtBefore = DateTime.Now;
                    // Write test data direct to Stream
                    DataGenerateToStream(ms, exampleData, rows, columnMap);
                    timeMs = (DateTime.Now - dtBefore).TotalMilliseconds;
                    Log("Complete. Size: " + ms.Length + " bytes. TimeTaken: " + timeMs + "ms");
                }

                //File.WriteAllBytes(ms.t
                String columns = PG_BinaryLoad.Copy_FromColsListGet(exampleData, serialColumns);
                String sql = "COPY " + table + columns + " FROM STDIN BINARY";

                TableCreate(con, exampleData); // Drop/Create the table
                DateTime dtSqlBefore = DateTime.Now;
                DbCopyQueryRun(con, ms, sql); // Load the data to DB
                double sqlTimeMs = (DateTime.Now - dtSqlBefore).TotalMilliseconds;
                double allTimeMs = (DateTime.Now - dtStart).TotalMilliseconds;

                Log("Stream size: " + String.Format("{0:n0}", ms.Length) + " bytes");
                Log("Stream time: " + String.Format("{0:n0}", timeMs) + "ms");
                Log("DB insert time: " + String.Format("{0:n0}", sqlTimeMs) + "ms");
                Log("Overall time: " + String.Format("{0:n0}", allTimeMs) + "ms");
            }
            catch (Exception e)
            {
                Log("ERROR: " + e.ToString());
            }
            finally
            {
                if (ms != null)
                {
                    ms.Close();
                }

                if (tempFile != null && tempFile.Length > 0)
                {
                    if (File.Exists(tempFile))
                    {
                        File.Delete(tempFile);
                    }
                }
            }
            Log("Press any key to return");
            Console.ReadKey();
        }

        private Stream TempStreamGet(String tempFile)
        {
            Stream ms = null;
            if (!String.IsNullOrEmpty(tempFile))
            {
                int bufSz = 104857600;
                String tmpFile = new FileInfo(tempFile).FullName;
                Log("File: " + tmpFile);
                if (File.Exists(tmpFile))
                {
                    File.Delete(tmpFile);
                }
                //ms = File.Create(tmpFile);
                ms = new FileStream(tmpFile, FileMode.CreateNew,
                    FileAccess.ReadWrite, FileShare.None, bufSz, FileOptions.Asynchronous);
            }
            else
            {
                ms = new MemoryStream();
            }
            return ms;
        }

        /// <summary>
        /// Connect to the PostgreSQL DB
        /// </summary>
        /// <param name="cStr"></param>
        /// <returns></returns>
        public NpgsqlConnection DbConnect(String cStr)
        {
            Log("Connecting to DB using: " + cStr);
            NpgsqlConnection con = new NpgsqlConnection(cStr);
            con.Open();
            Log("Connected");
            return con;
        }

        /// <summary>
        /// Load the Stream binary data in to PostgreSQL
        /// </summary>
        /// <param name="con"></param>
        /// <param name="ms"></param>
        /// <param name="sql"></param>
        public void DbCopyQueryRun(NpgsqlConnection con, Stream ms, String sql)
        {
            ms.Seek(0, SeekOrigin.Begin);
            NpgsqlCommand command = new NpgsqlCommand(sql, con);
            command.CommandTimeout = 0;
            NpgsqlCopyIn copyIn = new NpgsqlCopyIn(command, con, ms);

            double timeMs = -1;
            try
            {
                Log("Run SQL: '" + sql + "'");
                DateTime dateBefore = DateTime.Now;
                copyIn.Start();
                timeMs = (DateTime.Now - dateBefore).TotalMilliseconds;
                Log("Command completed");
            }
            catch (Exception e)
            {
                try
                {
                    copyIn.Cancel("Undo Copy");
                }
                catch (NpgsqlException ne)
                {
                    // This cancel request should generate an error
                    if (!ne.ToString().Contains("Undo Copy"))
                    {
                        throw new Exception("Failed to cancel query: " + ne + ". Failure: " + ne);
                    }
                    else
                    {
                        throw ne;
                    }
                }
                throw e;
            }
        }

        /// <summary>
        /// Generate test data directly to a memory stream (more efficient)
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="rows"></param>
        /// <param name="columnMap"></param>
        /// <returns></returns>
        private void DataGenerateToStream(Stream tempStream, DataTable dt, int rows, string columnMap)
        {
            // Parse columnMap in to a DataTable
            ColumnMapsParseSet(dt, columnMap);
            // Initialise the stream with a dataset containing schema
            PG_LoadState state = PG_BinaryLoad.Stream_Initialise(tempStream, dt);

            bool lastRow = false;
            // Add example data            
            for (int i = 0; i < rows; i++)
            {
                DataProgressLog(i);
                lastRow = i + 1 < rows ? false : true;
                object[] rowCols = testData.DataColumnLoad(dt, i);
                // Load a row in to the stream
                PG_BinaryLoad.Stream_AppendRow(state, rowCols, lastRow);
                Debug.WriteLine(i + " : " + PG_BinaryLoadTuple.ByteListToString(state.byList));
            }
            Debug.WriteLine("Output: " + PG_BinaryLoadTuple.ByteListToString(state.byList));
            // Set the stream end marker
            Stream ms = PG_BinaryLoad.Stream_End(state);
        }

        private void DataProgressLog(int i)
        {
            if (i % 50000 == 0)
            {
                if (i > 0)
                {
                    Log("Processing: " + i);
                }
            }
        }

        private List<String> SerialColumnNamesGet(DataColumnCollection columns)
        {
            List<String> serialColumns = new List<String>();
            foreach (DataColumn column in columns)
            {
                if (column.ExtendedProperties.ContainsKey(PG_BinaryLoad.PGRES_SERIAL))
                {
                    serialColumns.Add(column.ColumnName);
                }
            }
            return serialColumns;
        }

        /// <summary>
        /// Write DataTable to a CSV file
        /// </summary>
        /// <param name="exampleData"></param>
        /// <param name="csvInspect"></param>
        private void DataTableToFile(DataTable exampleData, string csvInspect)
        {
            String fileName = csvInspect + "" + exampleData.TableName + ".csv";
            String comma = "";

            Log("Write to inspect file: " + (new FileInfo(fileName)).FullName);
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(fileName, true))
            {
                foreach (DataRow row in exampleData.Rows)
                {
                    foreach (DataColumn col in exampleData.Columns)
                    {
                        comma = col == exampleData.Columns[exampleData.Columns.Count - 1] ? "" : ",";
                        file.WriteLine(row[col] + comma);
                    }
                }
            }
            Log("Inspect file write complete");
        }

        /// <summary>
        /// Get the PostgreSQL -> .NET data type mapping
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static Type ClrTypeFromPostgresType(String dbType)
        {
            if (dbType.Equals("jsonb"))
            {
                return typeof(String);
            } 
            else if (dbType.Equals("json"))
            {
                return typeof(String);
            }
            else if (dbType.Equals("boolean") || dbType.Equals("bool"))
            {
                return typeof(Boolean);
            }
            else if (dbType.Equals("integer") || dbType.Equals("int") || dbType.Equals("int4"))
            {
                return typeof(Int32);
            }
            else if (dbType.Equals("bigint") || dbType.Equals("int8"))
            {
                return typeof(Int64);
            }
            else if (dbType.Equals("text") || dbType.StartsWith("character") || dbType.StartsWith("varchar") || dbType.StartsWith("char"))
            {
                return typeof(String);
            }
            else if (dbType.StartsWith("numeric") || dbType.StartsWith("decimal"))
            {
                return typeof(Decimal);
            }
            else
            {
                throw new ApplicationException("Unsupported PostgreSQL -> .NET mapping type for: " + dbType);
            }
        }

        /// <summary>
        /// Generate some example data to a DataTable
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="rows"></param>
        /// <param name="columnMap"></param>
        /// <returns></returns>
        private DataTable DataGenRandomToTable(DataTable dt, int rows, string columnMap)
        {
            // Parse columnMap in to a DataTable
            ColumnMapsParseSet(dt, columnMap);

            // Add example data            
            for (int i = 0; i < rows; i++)
            {
                DataProgressLog(i);
                dt.Rows.Add(testData.DataColumnLoad(dt, i));
            }
            dt.AcceptChanges();
            return dt;
        }



        /// <summary>
        /// Parse the columnMap and create a DataTable reflecting its definition
        /// Add extended DataColumn info for serial columns (auto-increment)
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="columnMap"></param>
        public void ColumnMapsParseSet(DataTable dt, String columnMap)
        {
            List<String> mapList = new List<String>(columnMap.Split(new char[] { ',' }));
            // e.g. columnMap: column1=serial,column2=varchar(64):64,column3=int4,column4=numeric,column5=text:2048
            foreach (String mapEntry in mapList)
            {
                // e.g column1=serial
                String[] fields = mapEntry.Split(new char[] { '=' });
                if (fields.Length > 1)
                {
                    String[] fieldsVal = fields[1].Split(new char[] { ':' });
                    String pgresType = fieldsVal[0];
                    String[] lenFields = pgresType.Split(new char[] { '(' });
                    String lenStr = "";
                    if (lenFields.Length > 1) // e.g: character(64)
                    {
                        pgresType = lenFields[0];
                        lenStr = lenFields[1].Trim(new char[] { ')' });
                    }
                    Type columnType = typeof(int);
                    bool isSerial = pgresType.Equals(PG_BinaryLoad.PGRES_SERIAL);
                    if (!isSerial)
                    {
                        columnType = ClrTypeFromPostgresType(pgresType);
                    }

                    String columnName = fields[0];
                    if (!dt.Columns.Contains(columnName))
                    {
                        DataColumn column = dt.Columns.Add(columnName, columnType);
                        if (columnType == typeof(String))
                        {
                            column.MaxLength = lenFields.Length > 1 ? Int32.Parse(lenStr) : -1;
                        }
                        String ext = "";
                        if (fieldsVal.Length > 1)
                        {
                            ext = fieldsVal[1];
                            column.ExtendedProperties.Add(ext, ""); // Extra properties: e.g multibyte
                        }
                        if (isSerial) // set some extra meta-info for later
                        {
                            column.ExtendedProperties.Add(PG_BinaryLoad.PGRES_SERIAL, "");
                        }

                        column.ExtendedProperties[PG_BinaryLoad.PGRES_TYPE] = PG_BinaryLoad.PgresEnumFromType(pgresType);
                        Log("Column: " + columnName + " " + pgresType + "/" + columnType + " " + column.MaxLength + " " + ext);
                    }
                }
            }
        }

        /// <summary>
        /// Drop/Create PostgreSQL Table
        /// </summary>
        /// <param name="con"></param>
        /// <param name="dt"></param>
        private void TableCreate(NpgsqlConnection con, DataTable dt)
        {
            String createSql = SqlCreateFromDataTable(dt);
            Log("Drop/Create Table SQL: " + createSql);
            NpgsqlCommand cmd = new NpgsqlCommand(createSql, con);
            cmd.CommandTimeout = 0;
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Generate Drop/Create SQL for PostgreSQL from DataTable
        /// </summary>
        /// <param name="tbl"></param>
        /// <returns></returns>
        private String SqlCreateFromDataTable(DataTable tbl)
        {
            
            StringBuilder sql = new StringBuilder();
            sql.Append(SqlDropTableGet(tbl.TableName) + "CREATE TABLE " + tbl.TableName + " (\n");

            String colTyStr = "", comma = "";
            foreach (DataColumn col in tbl.Columns)
            {
                colTyStr = "" + PG_BinaryLoad.PgresTypeFromEnum((PgresType)col.ExtendedProperties[PG_BinaryLoad.PGRES_TYPE]);
                comma = (col == tbl.Columns[tbl.Columns.Count - 1]) ? "" : ",";
                sql.Append(col.ColumnName + " " + colTyStr + comma);
            }
            sql.Append("); ");
            if (tbl.ExtendedProperties["key"] != null)
            {
                sql.Append("CREATE INDEX " + tbl.TableName + "_IDX ON " + tbl.TableName + " (" + tbl.ExtendedProperties["key"] + ");");
            }
            return sql.ToString();
        }

        /// <summary>
        /// Generate drop SQL
        /// </summary>
        /// <param name="tblName"></param>
        /// <returns></returns>
        private String SqlDropTableGet(String tblName)
        {
            return "DROP INDEX IF EXISTS " + tblName + "_IDX ; DROP TABLE IF EXISTS " + tblName + " ;";
        }

        /// <summary>
        /// Log output to the console with a timestamp
        /// </summary>
        /// <param name="str"></param>
        private void Log(String msg)
        {
            DateTime dt = DateTime.Now;
            String str = String.Format("{0:MM/dd/yy H:mm:ss.FFF} - {1}",dt,msg);
            Console.WriteLine(str);
            Trace.WriteLine(str);
        }
    }
}
