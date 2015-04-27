using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CoreDataLibrary.Data;
using CoreDataLibrary.Helpers;

namespace CoreDataPopulateWorkItem
{
    internal static class Helpers
    {
        public static DataTable ToDataTable<T>(this IList<T> data)
        {

            PropertyDescriptorCollection properties =
                TypeDescriptor.GetProperties(typeof(T));
            DataTable table = new DataTable();
            foreach (PropertyDescriptor prop in properties)
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            foreach (T item in data)
            {
                DataRow row = table.NewRow();
                foreach (PropertyDescriptor prop in properties)
                    row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                table.Rows.Add(row);
            }
            return table;
        }

        public static void BulkAdd(DataTable dataTable)
        {
            Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "CoreData BulkAdd : " + dataTable.TableName);
            try
            {
                using (SqlConnection connection = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    connection.Open();
                    using (SqlBulkCopy s = new SqlBulkCopy(connection))
                    {
                        s.BulkCopyTimeout = 60000;

                        s.DestinationTableName = dataTable.TableName;

                        foreach (var column in dataTable.Columns)
                            s.ColumnMappings.Add(column.ToString(), column.ToString());

                        s.WriteToServer(dataTable);
                    }
                }
            }
            catch (Exception exception)
            {
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "OnStart BulkAdd Error : ", exception);
            }
        }

        public static void BulkAddTemp(DataTable dataTable)
        {
            Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "CoreData BulkAddTemp : " + dataTable.TableName);
            try
            {
                using (SqlConnection connection = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    connection.Open();
                    using (SqlBulkCopy s = new SqlBulkCopy(connection))
                    {
                        s.BulkCopyTimeout = 60000;

                        s.DestinationTableName = dataTable.TableName + "TEMP";

                        foreach (var column in dataTable.Columns)
                            s.ColumnMappings.Add(column.ToString(), column.ToString());

                        s.WriteToServer(dataTable);
                    }
                }
            }
            catch (Exception exception)
            {
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "OnStart BulkAdd Error : ", exception);
            }
        }

        public static void BulkAddDeleteFirst(DataTable dataTable)
        {
            Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "BulkAddDeleteFirst : " + dataTable.TableName);
            try
            {
                using (SqlConnection connection = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    connection.Open();
                    DeleteTableData(dataTable.TableName);
                    using (SqlBulkCopy s = new SqlBulkCopy(connection))
                    {
                        s.BulkCopyTimeout = 60000;

                        s.DestinationTableName = dataTable.TableName;

                        foreach (var column in dataTable.Columns)
                            s.ColumnMappings.Add(column.ToString(), column.ToString());

                        s.WriteToServer(dataTable);
                    }
                }
            }
            catch (Exception exception)
            {
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]:", "BulkAdd Error : " + exception);
            }
        }

        private static bool DeleteTableData(string tableName)
        {
            int rowCount = 0;
            try
            {
                string checkSql = "DELETE FROM " + tableName;
                using (SqlConnection connection = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand command = new SqlCommand(checkSql);
                    command.Connection = connection;

                    connection.Open();
                    command.CommandTimeout = 10000;

                    rowCount = Convert.ToInt32(command.ExecuteNonQuery());
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                return false;
            }
            return (rowCount > 0);
        }

        internal static void PopulateDropRename(DataTable propertiesTable)
        {
            BulkAddTemp(propertiesTable);
            DropOriginalTable(propertiesTable);
            RenameTempToOriginal(propertiesTable);
        }

        private static void DropOriginalTable(DataTable propertiesTable)
        {
            DropTable(propertiesTable.TableName);
        }

        private static void RenameTempToOriginal(DataTable propertiesTable)
        {
            RenameTable(propertiesTable.TableName + "TEMP", propertiesTable.TableName);
        }

        private static void RenameTable(string tableName, string oldTableName)
        {
            using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
            {
                using (var command = new SqlCommand())
                {
                    var sql = string.Format("EXEC sp_rename '" + tableName + "', '" + "'" + oldTableName + "'");
                    command.Connection = conn;
                    command.CommandText = sql;
                    conn.Open();
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void DropTable(string tableName)
        {
            using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
            {
                using (var command = new SqlCommand())
                {
                    var sql = string.Format("DROP TABLE {0}", tableName);
                    command.Connection = conn;
                    command.CommandText = sql;
                    conn.Open();
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
