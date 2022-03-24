using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace TheWheel.ETL.Providers
{
    public class DbReceiveOptions
    {
        public DbReceiveOptions()
        {

        }
        public DbReceiveOptions(DbQuery query, params SqlBulkCopyColumnMapping[] mappings)
        {
            this.query = query;
            this.mapping = mappings.Select(m =>
            {
                if (!m.SourceColumn.StartsWith("Then.") && !m.SourceColumn.StartsWith("Else."))
                    return m;
                if (m.DestinationOrdinal != -1)
                    return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring(5), m.DestinationOrdinal);
                return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring(5), m.DestinationColumn);
            }).ToArray();
        }

        public DbReceiveOptions(DbQuery query, IEnumerable<KeyValuePair<string, string>> mapping)
        : this(query, mapping.Select(m => new SqlBulkCopyColumnMapping(m.Key, m.Value)).ToArray())
        {
        }

        public DbQuery query;

        public SqlBulkCopyColumnMapping[] mapping;

        public static string BuildUpdateStatement(string tableName, params SqlBulkCopyColumnMapping[] mappings)
        {
            var sb = new StringBuilder("UPDATE ");
            sb.Append(tableName);
            sb.Append(" SET ");
            var isFirst = true;
            for (int i = 0; i < mappings.Length; i++)
            {
                if (mappings[i].SourceColumn.StartsWith("Then.") || mappings[i].SourceColumn.StartsWith("Else."))
                    continue;
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(", ");
                sb.Append(mappings[i].DestinationColumn);
                sb.Append('=');
                sb.Append("@__p" + i);
            }
            sb.Append(" WHERE ");
            isFirst = true;
            for (int i = 0; i < mappings.Length; i++)
            {
                if (!mappings[i].SourceColumn.StartsWith("Then.") && !mappings[i].SourceColumn.StartsWith("Else."))
                    continue;
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(" AND ");
                sb.Append(mappings[i].DestinationColumn);
                sb.Append('=');
                sb.Append("@__p" + i);
            }
            return sb.ToString();
        }


        public static string BuildInsertStatement(string tableName, params SqlBulkCopyColumnMapping[] mappings)
        {
            var sb = new StringBuilder("INSERT INTO ");
            sb.Append(tableName);
            sb.Append(" (");
            var isFirst = true;
            for (int i = 0; i < mappings.Length; i++)
            {
                if (mappings[i].SourceColumn.StartsWith("Then.") || mappings[i].SourceColumn.StartsWith("Else."))
                    continue;
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(", ");
                sb.Append(mappings[i].DestinationColumn);
                // sb.Append('=');
                // sb.Append("@__p" + i);
            }
            sb.Append(" ) VALUES ( ");
            isFirst = true;
            for (int i = 0; i < mappings.Length; i++)
            {
                if (mappings[i].SourceColumn.StartsWith("Then.") || mappings[i].SourceColumn.StartsWith("Else."))
                    continue;
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(", ");
                sb.Append("@__p" + i);
            }
            sb.Append(" )");
            return sb.ToString();
        }

        public static DbReceiveOptions Update(string tableName, params SqlBulkCopyColumnMapping[] mappings)
        {
            return new DbReceiveOptions(BuildUpdateStatement(tableName, mappings), mappings);
        }

        public static DbReceiveOptions Insert(string tableName, params SqlBulkCopyColumnMapping[] mappings)
        {
            return new DbReceiveOptions(BuildInsertStatement(tableName, mappings), mappings);
        }
    }
}