using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DRX.Database.PostgreSQL;
using PostgreSQL_Load;


namespace PG_BinaryLoad_Test
{
    public class PGTestData
    {
        private Random rand;

        public PGTestData()
        {
            rand = new Random(DateTime.Now.Millisecond);
        }

        /// <summary>
        /// Load random data in to a list of columns, based on the DataColumn.DataType
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public object[] DataColumnLoad(DataTable dt, int row)
        {
            object[] objectValues = new object[dt.Columns.Count];
            object colValue = null;
            for (int col = 0; col < dt.Columns.Count; col++)
            {
                DataColumn column = dt.Columns[col];
                String pgType = "" + PG_BinaryLoad.PgresTypeFromEnum((PgresType)column.ExtendedProperties[PG_BinaryLoad.PGRES_TYPE]);
                
                if (pgType.Equals("json"))
                {
                    colValue = RandomJsonGet();
                }
                else if (pgType.Equals("jsonb"))
                {
                    colValue = RandomJsonGet();
                }
                else if (column.DataType == typeof(String)) // text, character varying
                {
                    if (column.ExtendedProperties.ContainsKey("multibyte")) // use a multi byte string
                    {
                        colValue = RandomStringMultiByteGet(row);
                    }
                    else
                    {
                        colValue = RandomStringGet(column.MaxLength);
                    }
                }
                else if (column.DataType == typeof(Int32)) // integer
                {
                    colValue = RandomInt32Get();
                }
                else if (column.DataType == typeof(Int64)) // bigint
                {
                    colValue = RandomInt64Get();
                }
                else if (column.DataType == typeof(Decimal)) // numeric
                {
                    colValue = RandomDecimalGet();
                }
                else
                {
                    throw new ApplicationException("DataType not supported: " + column.DataType.ToString());
                }
                
                objectValues[col] = colValue;
            }
            return objectValues;
        }

        // TODO: Incomplete
        internal string RandomJsonGet()
        {
            // CREATE INDEX ON publishers((info->>'name'));
             
             return string.Format("{{" +
                "\"guid\": \"{0}\"," +
                "\"name\": \"{1}\"," +
                "\"active\": {2}," +
                "\"company\": \"{3}\"," +
                "\"address\": \"{4}\"," +
                "\"registered\": \"{5}\"," +
                "\"latitude\": {6}," +
                "\"longitude\": {7}," +
                "\"tags\": [" +
                    "\"{8}\"," +
                    "\"{9}\"," +
                    "\"{10}\"" +
                "]" +
            "}}",
           // String v = string.Format("{0}, {1},{2},{3},{4},{5},{6},{7},{8},{9},{10}",
            Guid.NewGuid(),
            RandomStringGet(12),
            RandomBoolGet().ToString().ToLower(),
            RandomStringGet(8),
            RandomStringGet(64),
            RandomDateTimeGet("yyyy-MM-ddTHH:mm:ss zzz"),
            RandomDecimalGet(),
            RandomDecimalGet(),
            RandomStringGet(8),
            RandomStringGet(8),
            RandomStringGet(8));
           // System.Console.WriteLine("json=" + v);
            //return v;
        }

        private Boolean RandomBoolGet()
        {
            return (this.rand.NextDouble() > 0.5);
        }

        private string RandomDateTimeGet(string fmt)
        {
            
            return new DateTime(RandomLongGet(DateTime.MaxValue.Ticks)).ToString(fmt);
        }

        private long RandomLongGet(long maxVal)
        {
            return (long)rand.Next() % (maxVal - 0);
        }

        /// <summary>
        /// Get the next Random Int32 number
        /// </summary>
        /// <returns></returns>
        private Int32 RandomInt32Get()
        {
            return rand.Next();
        }

        private List<String> variableStrings = new List<String>() {
            "¡¢£¹º»¼½¾¿ÀÁÂ", // latin1
            "ЌЎЏАБВГДЕ",      // cyrillic
            "ἅἆἇἈἉἊ",         // greekExt
            "⁼⁽⁾ⁿ₀₁₂",        // superSub
            "⅜⅝⅞⅟ⅠⅡⅢⅣ",   // numberForms
            "ڢڣڤڥڦڧڨک",      // arabic
            "丘 丙 业 丛 东 丝",// cjk
            "セゼソゾタ",       //katakana
            "כלםמןנס", // hebrew
             "６７８９：；＜＝＞？＠ＡＢＣ" // full width
         };

        private String RandomStringMultiByteGet(int rowNum)
        {
            return variableStrings[rowNum % 10];
        }

        private String RandomStringGet(int maxLength)
        {
            StringBuilder builder = new StringBuilder();
            char ch;

            for (int i = 0; i < maxLength; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * rand.NextDouble() + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }

        private Int64 RandomInt64Get()
        {
            return (rand.Next(-1000000000, 1000000000) * Int32.MaxValue);
        }

        private Decimal RandomDecimalGet()
        {
            return (rand.Next(-1000000000, 1000000000)) * Int64.MaxValue;
        }
    }
}
