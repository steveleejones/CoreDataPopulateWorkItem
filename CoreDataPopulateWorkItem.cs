using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CoreDataLibrary;
using CoreDataLibrary.Data;
using CoreDataLibrary.Helpers;
using CoreDataLibrary.Models;
using CoreDataPopulateWorkItem.Models;

namespace CoreDataPopulateWorkItem
{
    public class CoreDataPopulateWorkItem : BaseCoreWorkItem
    {
        private const long EXPECTED_PROP_DESC_CSV_SIZE = 2300000000;
        private const long EXPECTED_PROP_IMG_CSV_SIZE = 120000000;
        private static ReportLogger s_reportLogger;

        public override string Name
        {
            get { return "CoreDataPopulate"; }
        }

        public override bool DoWork(ReportLogger reportLogger)
        {
            s_reportLogger = reportLogger;

            int stepId = s_reportLogger.AddStep("Updating CoreData");
            try
            {
                if (CoreDataLib.LchAndLcbAvailableOnIVDB())
                {
                    List<Task> tasks = new List<Task>();
                    tasks.Add(Task.Factory.StartNew(Populate));
                    tasks.Add(Task.Factory.StartNew(PopulateHighQualityImagesLists));

                    Task.WaitAll(tasks.ToArray());
                    Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message ", "End of DailyRun");
                    s_reportLogger.EndStep(stepId, "CoreData Update completed");
                }
                else
                {
                    s_reportLogger.EndStep(stepId, "Unable to connect to LCB/LCH check availability");
                    Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message ", "Unable to connect to LCB/LCH check availability");
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error ", "DailyRun", exception);
                return false;
            }
            return true;
        }

        private static void Populate()
        {
            PopulatePropertyUrls();
            PopulatePropertyTypes();
            PopulateProperties();

            PopulateAllDescriptions();

            PopulatePropertyImages();

            PopulatePropertyFacilities();
            PopulateFacilityGroups();
            PopulateFacility();
            PopulateFacilityLanguage();
            PopulateAirportValues();
            PopulateResortAirport();
            PopulateFacilitiesLists();
            PopulateImagesLists();
        }

        private static void PopulatePropertyImages()
        {
            if (ExtractPropertyImagesToCsv() >= EXPECTED_PROP_IMG_CSV_SIZE)
            {
                PopulateCorePropertyImages();
                TidyUpImages();
            }
        }

        private static void PopulateAllDescriptions()
        {
            if (ExtractPropertyDescriptionsToCsv() >= EXPECTED_PROP_DESC_CSV_SIZE)
            {
                PopulateCoreDataWithPropertyDescriptions();
                PopulateCoreDataWithUKPropertyDescriptions();
            }
        }

        private static void PopulatePropertyUrls()
        {
            int stepId = s_reportLogger.AddStep();
            List<PropertyUrl> propertyUrlList = new List<PropertyUrl>();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT BrandID, LanguageID, ID AS PropertyReferenceId, URL, RedirectURL FROM [IVDB].[LCH].[dbo].[URLRewrite] WHERE DataObject = 'PropertyReference'";
                    sqlCommand.CommandText = sql;
                    conn.Open();
                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyUrl propertyUrl = new PropertyUrl();

                        propertyUrl.BrandID = Convert.ToInt32(reader["BrandId"].ToString());
                        propertyUrl.LanguageID = Convert.ToInt32(reader["LanguageId"].ToString());
                        propertyUrl.PropertyReferenceId = Convert.ToInt32(reader["PropertyReferenceId"].ToString());
                        propertyUrl.URL = reader["URL"].ToString();
                        propertyUrl.RedirectURL = reader["RedirectURL"].ToString();

                        propertyUrlList.Add(propertyUrl);
                    }
                    reader.Close();

                    DataTable propertyUrlsTable = Helpers.ToDataTable(propertyUrlList);
                    propertyUrlsTable.TableName = "PropertyUrls";
                    if (propertyUrlsTable.Rows.Count > 10000)
                    {
                        Helpers.BulkAddDeleteFirst(propertyUrlsTable);
                    }
                    else
                    {
                        ReportLogger.AddMessage("CoreData", "PropertyUrls not updated");
                    }
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulatePropertyUrls", exception);
            }
        }

        private static void PopulatePropertyTypes()
        {
            int stepId = s_reportLogger.AddStep();
            List<PropertyTypes> propertyTypesList = new List<PropertyTypes>();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT PropertyTypeID, PropertyType FROM [IVDB].[LCH].[dbo].[PropertyType]";
                    sqlCommand.CommandText = sql;
                    conn.Open();
                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyTypes propertyTypes = new PropertyTypes();

                        propertyTypes.PropertyTypeID = Convert.ToInt32(reader["PropertyTypeID"].ToString());
                        propertyTypes.PropertyType = reader["PropertyType"].ToString();

                        propertyTypesList.Add(propertyTypes);
                    }
                    reader.Close();

                    DataTable propertyTypesTable = Helpers.ToDataTable(propertyTypesList);
                    propertyTypesTable.TableName = "PropertyTypes";
                    if (propertyTypesTable.Rows.Count > 10)
                    {
                        Helpers.BulkAddDeleteFirst(propertyTypesTable);
                    }
                    else
                    {
                        ReportLogger.AddMessage("CoreData", "PropertyUrls not updated");
                    }
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->TidyUpImages", exception);
            }
        }

        private static void PopulateCoreDataWithUKPropertyDescriptions()
        {
            List<PropertyDescription> propertyUKDescriptionList = new List<PropertyDescription>();
            Dictionary<int, PropertyDescription> uKPropertyDescriptionDictionary = new Dictionary<int, PropertyDescription>();


            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @" SELECT 
 SourceID AS PropertyReferenceId,
 PageTitle AS PageTitle,
 URL AS URL,
 MetaDescription AS MetaDescription,
 Name AS Name,
 Summary AS Summary,
 Description AS Description,
 DistanceFromAirport AS DistanceFromAirport,
 TransferTime AS TransferTime,
 RightChoice AS RightChoice,
 LocationAndResort AS LocationAndResort,
 EatingAndDrinking AS EatingAndDrinking,
 Accommodation AS Accommodation,
 SuitableFor AS SuitableFor,
 SwimmingPools AS SwimmingPools
 FROM ivdb.lcb.dbo.CMS_PropertyReference";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyDescription propertyDescription = new PropertyDescription();

                        propertyDescription.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceId"]);
                        propertyDescription.LanguageId = 1;
                        propertyDescription.PageTitle = reader["PageTitle"].ToString();
                        SqlString urlString = SqlString.Null;
                        urlString = reader["URL"].ToString();
                        if (urlString.IsNull)
                            propertyDescription.Url = "";
                        else
                            propertyDescription.Url = urlString.ToString();
                        propertyDescription.MetaDescription = reader["MetaDescription"].ToString();
                        propertyDescription.Name = reader["Name"].ToString();
                        propertyDescription.Summary = reader["Summary"].ToString();
                        propertyDescription.Description = reader["Description"].ToString();
                        propertyDescription.DistanceFromAirport = reader["DistanceFromAirport"].ToString();
                        propertyDescription.TransferTime = reader["TransferTime"].ToString();
                        propertyDescription.RightChoice = reader["RightChoice"].ToString();
                        propertyDescription.LocationAndResort = reader["LocationAndResort"].ToString();
                        propertyDescription.EatingAndDrinking = reader["EatingAndDrinking"].ToString();
                        propertyDescription.Accomodation = reader["Accommodation"].ToString();
                        propertyDescription.SuitableFor = reader["SuitableFor"].ToString();
                        propertyDescription.SwimmingPools = reader["SwimmingPools"].ToString();

                        propertyUKDescriptionList.Add(propertyDescription);
                        if (!uKPropertyDescriptionDictionary.ContainsKey(propertyDescription.PropertyReferenceId))
                        {
                            uKPropertyDescriptionDictionary.Add(propertyDescription.PropertyReferenceId, propertyDescription);
                        }
                        else
                        {
                            if (propertyDescription.PageTitle != String.Empty)
                            {
                                uKPropertyDescriptionDictionary.Remove(propertyDescription.PropertyReferenceId);
                                uKPropertyDescriptionDictionary.Add(propertyDescription.PropertyReferenceId, propertyDescription);
                            }
                        }

                    }
                    reader.Close();
                }
                //propertyUKDescriptionList = uKPropertyDescriptionDictionary.Values.ToList();
                //DataTable propertyDescriptionsUKTable = Helpers.ToDataTable(propertyUKDescriptionList);
                DataTable propertyDescriptionsUKTable = Helpers.ToDataTable(uKPropertyDescriptionDictionary.Values.ToList());
                propertyDescriptionsUKTable.TableName = "PropertyDescription";
                if (propertyDescriptionsUKTable.Rows.Count > 150000)
                {
                    Helpers.BulkAdd(propertyDescriptionsUKTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "PropertyDescription not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception e)
            {
                s_reportLogger.EndStep(stepId, e);
                Console.WriteLine(e);
            }
        }

        private static void TidyUpImages()
        {
            int stepId = s_reportLogger.AddStep();
            RemovePropertiesNotRequired();
            RemoveDuplicateImages();
            s_reportLogger.EndStep(stepId);
        }

        private static void RemoveDuplicateImages()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    if (TableExists("PropertyImagesTemp"))
                        DropTable("PropertyImagesTemp");

                    string sql = @"SELECT DISTINCT OtherImages, PropertyReferenceId, Title, AdditionalInformation INTO PropertyImagesTemp FROM PropertyImages";
                    sqlCommand.CommandText = sql;
                    conn.Open();
                    sqlCommand.ExecuteNonQuery();

                    if (TableExists("PropertyImages"))
                        DropTable("PropertyImages");

                    sql = "SELECT PropertyReferenceId, OtherImages, Title, AdditionalInformation INTO [dbo].[PropertyImages] FROM [dbo].[PropertyImagesTemp]";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();

                    if (TableExists("PropertyImagesTemp"))
                        DropTable("PropertyImagesTemp");
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->TidyUpImages", exception);
            }
        }

        private static bool TableExists(string tablename)
        {
            using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
            {
                using (var command = new SqlCommand())
                {
                    var sql = string.Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'", tablename);
                    command.Connection = conn;
                    command.CommandText = sql;
                    conn.Open();
                    var count = Convert.ToInt32((command.ExecuteScalar()));
                    return (count > 0);
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

        private static void RemoveDuplicateResortAirports()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    if (TableExists("ResortAirportTemp"))
                        DropTable("ResortAirportTemp");

                    string sql = @"SELECT MAX(AirportId) AS AirportId, MAX(ResortId) AS ResortId, MAX(ResortName) AS ResortName 
INTO ResortAirportTemp FROM ResortAirport GROUP BY ResortId";
                    sqlCommand.CommandText = sql;
                    conn.Open();
                    sqlCommand.ExecuteNonQuery();

                    sql = "DROP TABLE ResortAirport";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                    sql = "SELECT AirportId, ResortId, ResortName INTO ResortAirport FROM ResortAirportTemp";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                    sql = "DROP TABLE ResortAirportTemp";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->RemoveDuplicateResortAirports", exception);
            }
        }

        private static void RemovePropertiesNotRequired()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    string sql = @"DELETE FROM [CoreData].[dbo].[PropertyImages]
WHERE PropertyReferenceId NOT IN (SELECT PropertyReferenceId FROM CoreData.dbo.Properties)";

                    sqlCommand.CommandText = sql;
                    conn.Open();
                    sqlCommand.ExecuteNonQuery();
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->RemovePropertiesNotRequired", exception);
            }
        }

        private static void PopulateCorePropertyImages()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    conn.Open();

                    string sql = "DELETE FROM PropertyImages";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();

                    if (CoreDataLib.IsLive())
                        sql = @" BULK INSERT [CoreData].[dbo].[PropertyImages] FROM 'E:\CoreData\ExportFiles\PropertyImages.csv' WITH ( BATCHSIZE = 1000, CODEPAGE = 'ACP', FIELDTERMINATOR = '!|!', ROWTERMINATOR = '(!!!!!!!!!!)')";
                    else
                        sql = @" BULK INSERT [CoreData_Test].[dbo].[PropertyImages] FROM 'E:\CoreData\ExportFiles\PropertyImages.csv' WITH ( BATCHSIZE = 1000, CODEPAGE = 'ACP', FIELDTERMINATOR = '!|!', ROWTERMINATOR = '(!!!!!!!!!!)')";
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateCorePropertyImages", exception);
            }
        }

        private static long ExtractPropertyImagesToCsv()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    string sql;
                    if (CoreDataLib.IsLive())
                        sql = @"DECLARE @bcpCmd as VARCHAR(8000); SET @bcpCmd= 'bcp ""SET NOCOUNT ON;SELECT pr.PropertyReferenceID,''http://content.lowcostholidays.com/'' +  oim.OtherImages AS OtherImages, oim.OtherImages_ImageTitle AS Title, oim.OtherImages_AdditionalInfo AS AdditionalInfo FROM LCB.dbo.CMS_PropertyReference_OtherImages oim JOIN lcb.dbo.CMS_PropertyReference cmspr ON cmspr.CMS_PropertyReferenceID = oim.CMS_PropertyReferenceID JOIN lcb.dbo.PropertyReference pr ON pr.PropertyReferenceId = cmspr.SourceID WHERE pr.CurrentPropertyReference = 1;SET NOCOUNT OFF;"" queryout E:\Coredata\ExportFiles\PropertyImages.csv -t ""!|!"" -r (!!!!!!!!!!) -c -C ACP -S IVDB -U LCMI -P l0wcostM!'; EXEC master..xp_cmdshell @bcpCmd;";
                    else
                        sql = @"DECLARE @bcpCmd as VARCHAR(8000); SET @bcpCmd= 'bcp ""SET NOCOUNT ON;SELECT pr.PropertyReferenceID,''http://content.lowcostholidays.com/'' +  oim.OtherImages AS OtherImages, oim.OtherImages_ImageTitle AS Title, oim.OtherImages_AdditionalInfo AS AdditionalInfo FROM LCB.dbo.CMS_PropertyReference_OtherImages oim JOIN lcb.dbo.CMS_PropertyReference cmspr ON cmspr.CMS_PropertyReferenceID = oim.CMS_PropertyReferenceID JOIN lcb.dbo.PropertyReference pr ON pr.PropertyReferenceId = cmspr.SourceID WHERE pr.CurrentPropertyReference = 1;SET NOCOUNT OFF;"" queryout E:\Coredata\ExportFiles\Test\PropertyImages.csv -t ""!|!"" -r (!!!!!!!!!!) -c -C ACP -S IVDB -U LCMI -P l0wcostM!'; EXEC master..xp_cmdshell @bcpCmd;";

                    sqlCommand.CommandText = sql;

                    conn.Open();

                    sqlCommand.ExecuteNonQuery();

                    FileInfo fileInfo;
                    if (CoreDataLib.IsLive())
                        fileInfo = new FileInfo(@"E:\CoreData\ExportFiles\PropertyImages.csv");
                    else
                        fileInfo = new FileInfo(@"\\SVRsql4\E$\CoreData\ExportFiles\Test\PropertyImages.csv");

                    if (fileInfo.Exists)
                    {
                        s_reportLogger.EndStep(stepId);
                        return fileInfo.Length;
                    }
                    s_reportLogger.EndStep(stepId, "File PropertyImages.csv could not be found.");

                    return 0;
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData Error [SVRSQL4]:", "Process->ExtractPropertyImagesToCsv", exception);
            }
            return 0;
        }

        private static long ExtractPropertyDescriptionsToCsv()
        {

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.Connection = conn;
                    sqlCommand.CommandTimeout = 60000;

                    sqlCommand.CommandType = CommandType.Text;
                    string sql;
                    if (CoreDataLib.IsLive())
                        sql = @"DECLARE @bcpCmd as VARCHAR(8000); SET @bcpCmd= 'bcp ""SET NOCOUNT ON;SELECT prr.PropertyReferenceId AS PropertyReferenceId, prl.languageId AS LanguageId, prl.Pagetitle AS PageTitle, prl.URL AS Url, prl.MetaDescription AS MetaDescription, prl.Name AS Name, prl.Summary AS Summary, prl.Description AS Description, prl.DistanceFromAirport AS DistanceFromAirport, prl.TransferTime AS TransferTime, prl.RightChoice AS RightChoice, prl.LocationAndResort AS LocationAndResort, prl.EatingAndDrinking AS EatingAndDrinking, prl.Accommodation AS Accomodation, prl.SuitableFor AS SuitableFor, prl.SwimmingPools AS SwimmingPools FROM  lcb.dbo.CMS_PropertyReference pr join lcb.dbo.PropertyReference prr ON prr.propertyreferenceid = pr.SourceId join lcb.dbo.CMS_PropertyReference_Language prl ON pr.Id = prl.Id WHERE prr.CurrentPropertyReference = 1 AND prl.LanguageId != 1 ORDER BY PropertyReferenceId;SET NOCOUNT OFF;"" queryout E:\CoreData\ExportFiles\PropertyDescription.csv -t ""!|!"" -r (!!!!!!!!!!) -c -C ACP -S IVDB -U LCMI -P l0wcostM!'; EXEC master..xp_cmdshell @bcpCmd;";
                    else
                        sql = @"DECLARE @bcpCmd as VARCHAR(8000); SET @bcpCmd= 'bcp ""SET NOCOUNT ON;SELECT prr.PropertyReferenceId AS PropertyReferenceId, prl.languageId AS LanguageId, prl.Pagetitle AS PageTitle, prl.URL AS Url, prl.MetaDescription AS MetaDescription, prl.Name AS Name, prl.Summary AS Summary, prl.Description AS Description, prl.DistanceFromAirport AS DistanceFromAirport, prl.TransferTime AS TransferTime, prl.RightChoice AS RightChoice, prl.LocationAndResort AS LocationAndResort, prl.EatingAndDrinking AS EatingAndDrinking, prl.Accommodation AS Accomodation, prl.SuitableFor AS SuitableFor, prl.SwimmingPools AS SwimmingPools FROM  lcb.dbo.CMS_PropertyReference pr join lcb.dbo.PropertyReference prr ON prr.propertyreferenceid = pr.SourceId join lcb.dbo.CMS_PropertyReference_Language prl ON pr.Id = prl.Id WHERE prr.CurrentPropertyReference = 1 AND prl.LanguageId != 1 ORDER BY PropertyReferenceId;SET NOCOUNT OFF;"" queryout E:\CoreData\ExportFiles\Test\PropertyDescription.csv -t ""!|!"" -r (!!!!!!!!!!) -c -C ACP -S IVDB -U LCMI -P l0wcostM!'; EXEC master..xp_cmdshell @bcpCmd;";


                    sqlCommand.CommandText = sql;

                    conn.Open();

                    sqlCommand.ExecuteNonQuery();

                    FileInfo fileInfo;
                    if (CoreDataLib.IsLive())
                        fileInfo = new FileInfo(@"E:\CoreData\ExportFiles\PropertyDescription.csv");
                    else
                        fileInfo = new FileInfo(@"\\SVRsql4\E$\CoreData\ExportFiles\Test\PropertyDescription.csv");

                    if (fileInfo.Exists)
                    {
                        s_reportLogger.EndStep(stepId);
                        return fileInfo.Length;
                    }
                    else
                        Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData [SVRSQL4]: Error", "Process->ExtractPropertyDescriptionsToCsv [file not found on server]");

                    s_reportLogger.EndStep(stepId, "Process->ExtractPropertyDescriptionsToCsv [file not found on server]");
                    return 0;
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->ExtractPropertyDescriptionsToCsv", exception);
            }
            return 0;
        }

        private static void PopulateCoreDataWithPropertyDescriptions()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    DropCreatePropertyDescriptions();
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    conn.Open();

                    // NOTE may not be required now after updating the Stored Procedure called in DropCreatePropertyDescriptions above.
                    //string sql = @"DELETE FROM PropertyDescription";
                    //sqlCommand.CommandText = sql;
                    //sqlCommand.ExecuteNonQuery();

                    string sql = "";
                    if (CoreDataLib.IsLive())
                        sql = @" BULK INSERT [CoreData].[dbo].[PropertyDescription] FROM 'E:\CoreData\ExportFiles\PropertyDescription.csv' WITH ( BATCHSIZE = 1000, CODEPAGE = 'ACP', FIELDTERMINATOR = '!|!', ROWTERMINATOR = '(!!!!!!!!!!)')";
                    else
                        sql = @" BULK INSERT [CoreData_Test].[dbo].[PropertyDescription] FROM 'E:\CoreData\ExportFiles\Test\PropertyDescription.csv' WITH ( BATCHSIZE = 1000, CODEPAGE = 'ACP', FIELDTERMINATOR = '!|!', ROWTERMINATOR = '(!!!!!!!!!!)')";

                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                    s_reportLogger.EndStep(stepId);
                }
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateCoreDataWithPropertyDescriptions", exception);
            }
        }

        private static void DropCreatePropertyDescriptions()
        {
            using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
            {
                SqlCommand sqlCommand = new SqlCommand("dbo.uspDropRecreatePropertyDescription");
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.CommandTimeout = 60000;
                sqlCommand.Connection = conn;

                conn.Open();

                sqlCommand.ExecuteNonQuery();
            }
        }

        private static void PopulatePropertyFacilities()
        {
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    DataTable facilityTable = new DataTable("PropertyFacilities");
                    string sql = "SELECT FacilityID AS FacilityId, PropertyID AS PropertyReferenceId FROM ivdb.lcb.dbo.PropertyFacility";

                    SqlCommand sqlCommand = new SqlCommand(sql, conn);
                    conn.Open();
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlCommand);
                    dataAdapter.Fill(facilityTable);
                    if (facilityTable.Rows.Count >= 200000)
                    {
                        Helpers.BulkAddDeleteFirst(facilityTable);
                    }
                    else
                    {
                        Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Warning", "Process->PopulatePropertyFacilities - (Facilties table not updated)");
                        ReportLogger.AddMessage("CoreData", "PropertyFacilities not updated");
                    }
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulatePropertyFacilities", exception);
            }
        }

        public static void PopulateProperties()
        {
            int stepId = s_reportLogger.AddStep();
            List<Models.Property> propertyList = new List<Models.Property>();
            Dictionary<int, Models.Property> propertyDictionary = new Dictionary<int, Models.Property>();
            List<PropertyDetails> propertyDetailList = new List<PropertyDetails>();
            Dictionary<int, PropertyDetails> propertyDeatilsDictionary = new Dictionary<int, PropertyDetails>();

            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand("dbo.uspPopulateProperties");
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    sqlCommand.CommandTimeout = 60000;
                    sqlCommand.Connection = conn;

                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        Models.Property property = new Models.Property();
                        PropertyDetails propertyDetails = new PropertyDetails();
                        property.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceId"]);
                        propertyDetails.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceId"]);
                        property.PropertyName = reader["PropertyName"].ToString();
                        propertyDetails.LanguageId = 1;
                        propertyDetails.Address1 = reader["Address1"].ToString().Replace("\"", "'");
                        //// NOTE : Instead of stripping out '|' in the SQL using REPLACE we could do it here in the code instead.
                        //propertyDetails.Address1 = reader["Address1"].ToString().Replace("\"", "'").Replace("|","");
                        propertyDetails.Address2 = reader["Address2"].ToString().Replace("\"", "'");
                        propertyDetails.TownCity = reader["TownCity"].ToString().Replace("\"", "'");
                        propertyDetails.County = reader["County"].ToString().Replace("\"", "'");
                        propertyDetails.PostcodeZip = reader["PostcodeZip"].ToString().Replace("\"", "'");
                        propertyDetails.Telephone = reader["Telephone"].ToString();
                        propertyDetails.Fax = reader["Fax"].ToString();
                        propertyDetails.Latitude = reader["Latitude"] == DBNull.Value
                            ? 0.0
                            : Convert.ToDouble(reader["Latitude"]);
                        propertyDetails.Longitude = reader["Longitude"] == DBNull.Value
                            ? 0.0
                            : Convert.ToDouble(reader["Longitude"]);
                        propertyDetails.PropertyTypeId = reader["PropertyTypeID"] == DBNull.Value
                            ? 1
                            : Convert.ToInt32(reader["PropertyTypeID"]);
                        propertyDetails.PropertyType = reader["PropertyType"].ToString();
                        propertyDetails.MainImage = "http://content.lowcostholidays.com/" + reader["MainImage"].ToString();
                        propertyDetails.Strapline = reader["Strapline"].ToString();
                        propertyDetails.MainImageThumbnail = "http://content.lowcostholidays.com/" + reader["MainImageThumbnail"].ToString();
                        property.Resort = reader["ResortExclusion"].ToString();
                        property.Region = reader["RegionExclusion"].ToString();
                        property.Country = reader["CountryExclusion"].ToString();
                        property.ResortId = reader["ResortId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["ResortId"]);
                        property.RegionId = reader["RegionId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["RegionId"]);
                        property.CountryCode = reader["CountryCode"] == DBNull.Value
                            ? ""
                            : reader["CountryCode"].ToString();
                        property.CountryId = reader["CountryId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["CountryId"]);
                        property.CountryCode = reader["CountryCode"].ToString();
                        property.IncludesOwnStock = reader["IncludesOwnStock"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["IncludesOwnStock"]);
                        property.BestSeller = reader["BestSeller"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["BestSeller"]);
                        property.Rating = reader["Rating"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["Rating"]);
                        property.ContractCount = reader["ContractCount"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["ContractCount"]);
                        if (!propertyDictionary.ContainsKey(property.PropertyReferenceId))
                            propertyDictionary.Add(property.PropertyReferenceId, property);
                        if (!propertyDeatilsDictionary.ContainsKey(property.PropertyReferenceId))
                            propertyDeatilsDictionary.Add(property.PropertyReferenceId, propertyDetails);
                    }
                    reader.Close();
                    propertyList = propertyDictionary.Values.ToList();
                    propertyDetailList = propertyDeatilsDictionary.Values.ToList();
                    DataTable propertiesTable = Helpers.ToDataTable(propertyList);
                    DataTable propertyDetailsTable = Helpers.ToDataTable(propertyDetailList);
                    propertiesTable.TableName = "Properties";
                    propertyDetailsTable.TableName = "PropertyDetails";
                    if (propertiesTable.Rows.Count > 150000)
                    {
                        // Populate temp table.
                        // Drop original table.
                        // Rename temp table.
                        //Helpers.PopulateDropRename(propertiesTable);
                        //Helpers.PopulateDropRename(propertyDetailsTable);
                        Helpers.BulkAddDeleteFirst(propertiesTable);
                        Helpers.BulkAddDeleteFirst(propertyDetailsTable);
                    }
                    else
                    {
                        Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Warning", "Process->PopulateProperties (Properties table not updated)");
                        ReportLogger.AddMessage("CoreData", "Properties table was not updated");
                    }

                    Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message", "Number of PropertyDetails Added : " + propertyDetailList.Count);
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateProperties", exception);
            }
        }

        public static void PopulateLanguages()
        {
            List<Language> languages = new List<Language>();
            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @" SELECT LanguageId, LanguageCode, Language AS LanguageName, CultureCode FROM IVDB.LCH.dbo.Language";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        Language language = new Language();
                        language.LanguageId = reader["LanguageId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["LanguageId"]);
                        language.LanguageCode = reader["LanguageCode"].ToString();
                        language.LanguageName = reader["LanguageName"].ToString();
                        language.CultureCode = reader["CultureCode"].ToString();
                        language.Encoding = "windows-1252";
                        languages.Add(language);
                    }
                    reader.Close();
                }
                DataTable languageTable = Helpers.ToDataTable(languages);
                languageTable.TableName = "Language";
                if (languageTable.Rows.Count > 50)
                {
                    Helpers.BulkAddDeleteFirst(languageTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "Language not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateLanguages", exception);
            }
        }

        public static void PopulateFacilityGroups()
        {
            List<FacilitiesGroup> facilityGroups = new List<FacilitiesGroup>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT FacilityGroupID, FacilityGroup FROM IVDB.LCH.dbo.FacilityGroup";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        FacilitiesGroup facilitiesGroup = new FacilitiesGroup();
                        facilitiesGroup.FacilityGroupId = reader["FacilityGroupId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["FacilityGroupId"]);
                        facilitiesGroup.FacilityGroup = reader["FacilityGroup"].ToString();
                        facilityGroups.Add(facilitiesGroup);
                    }
                    reader.Close();
                }
                DataTable facilityGroupTable = Helpers.ToDataTable(facilityGroups);
                facilityGroupTable.TableName = "FacilityGroups";
                if (facilityGroupTable.Rows.Count > 0)
                {
                    Helpers.BulkAddDeleteFirst(facilityGroupTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "FacilityGroups not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateFacilityGroups", exception);
            }
        }

        public static void PopulateFacility()
        {
            List<Facility> facilities = new List<Facility>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT FacilityID, FacilityGroupId FROM IVDB.LCH.dbo.Facility";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        Facility facility = new Facility();
                        facility.FacilityId = reader["FacilityId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["FacilityId"]);
                        facility.FacilityGroupId = reader["FacilityGroupId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["FacilityGroupId"]);
                        facility.LanguageId = 1;
                        facilities.Add(facility);
                    }
                    reader.Close();
                }
                DataTable facilityTable = Helpers.ToDataTable(facilities);
                facilityTable.TableName = "Facility";
                if (facilityTable.Rows.Count > 30)
                {
                    Helpers.BulkAddDeleteFirst(facilityTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "Facility not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateFacility", exception);
            }
        }

        public static void PopulateFacilityLanguage()
        {
            List<FacilityLanguage> facilityLanguages = new List<FacilityLanguage>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT FacilityID, Facility FROM  IVDB.LCH.dbo.Facility";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    conn.Open();

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        FacilityLanguage facilityLanguage = new FacilityLanguage();
                        facilityLanguage.FacilityId = reader["FacilityId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["FacilityId"]);
                        facilityLanguage.FacilityDescription = reader["Facility"].ToString();
                        facilityLanguage.LanguageId = 1;
                        facilityLanguages.Add(facilityLanguage);
                    }
                    reader.Close();

                    sql = @"SELECT d.LanguageID AS LanguageId, c.FacilityID AS FacilityId, b.Translation AS Facility
  FROM IVDB.LCH.dbo.Translation a
      inner join IVDB.LCH.dbo.TranslationLookup b
            on a.TranslationID=b.TranslationID
      right join IVDB.lch.dbo.Facility c
            on b.SourceID=c.FacilityID
      left join IVDB.lch.dbo.Language d
            on d.LanguageID=b.LanguageID
where a.Translation = 'facility'";

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;
                    if (conn.State == ConnectionState.Closed)
                        conn.Open();

                    reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        FacilityLanguage facilityLanguage = new FacilityLanguage();
                        facilityLanguage.FacilityId = reader["FacilityId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["FacilityId"]);
                        facilityLanguage.FacilityDescription = reader["Facility"].ToString();
                        facilityLanguage.LanguageId = reader["LanguageId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["LanguageId"]);
                        facilityLanguages.Add(facilityLanguage);
                    }
                    reader.Close();
                }

                DataTable facilityLanguageTable = Helpers.ToDataTable(facilityLanguages);
                facilityLanguageTable.TableName = "FacilityLanguage";
                if (facilityLanguageTable.Rows.Count > 100)
                {
                    Helpers.BulkAddDeleteFirst(facilityLanguageTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "FacilityLanguage not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateFacilityLanguage", exception);
            }
        }

        public static void PopulateTTIValues()
        {
            int stepId = s_reportLogger.AddStep();
            List<PropertyTTI> propertyGiataList = new List<PropertyTTI>();

            //FileDownloader.DownloadFile(@"http://tticodes.giatamedia.com/webservice/rest/1.0/properties/properties.xml", "C:\tti.xml");
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    string sql = @"select distinct c.PropertyReferenceID, a.TPPropertyReferenceID 
from ivdb.lch.dbo.TPProperty a 
inner join ivdb.lcb.dbo.PropertyDedupe b 
on a.TPPropertyID=b.PropertyID 
inner join ivdb.lcb.dbo.propertyreference c 
on b.PropertyReferenceID=c.PropertyReferenceID 
where TPPropertyReferenceID is not null
and c.PropertyReferenceID IN (select PropertyReferenceId from [CoreData].[dbo].[Properties])";

                    conn.Open();
                    SqlCommand sqlCommand = new SqlCommand(sql, conn);
                    sqlCommand.CommandTimeout = 60000;

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyTTI propTti = new PropertyTTI();

                        propTti.PropertyReferenceId = reader["PropertyReferenceID"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceID"]);
                        propTti.TTIId = reader["TPPropertyReferenceID"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["TPPropertyReferenceID"]);

                        propertyGiataList.Add(propTti);
                    }
                    reader.Close();
                }
                DataTable propertyGiataTable = Helpers.ToDataTable(propertyGiataList);
                propertyGiataTable.TableName = "PropertyTTI";
                if (propertyGiataTable.Rows.Count > 150000)
                {
                    Helpers.BulkAddDeleteFirst(propertyGiataTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "PropertyTTI not updated");
                }
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message", "Number of TTIValues Added : " + propertyGiataList.Count);
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateTTIValues", exception);
            }
        }

        public static void PopulateAirportValues()
        {
            List<Airport> airportList = new List<Airport>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    string sql = @"SELECT AirportID, IATACode, Airport FROM [IVDB].[LCH].[dbo].[Airport]";
                    conn.Open();
                    SqlCommand sqlCommand = new SqlCommand(sql, conn);
                    sqlCommand.CommandTimeout = 60000;

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        Airport airport = new Airport();

                        airport.AirportId = reader["AirportId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["AirportId"]);
                        airport.IATACode = reader["IATACode"].ToString();
                        airport.AirportName = reader["Airport"].ToString();

                        airportList.Add(airport);
                    }
                    reader.Close();
                }
                DataTable airportTable = Helpers.ToDataTable(airportList);
                airportTable.TableName = "Airports";
                if (airportTable.Rows.Count > 900)
                {
                    Helpers.BulkAddDeleteFirst(airportTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "Airports not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateAirportValues", exception);
            }
        }

        private static void PopulateResortAirport()
        {
            List<ResortAirport> resortAirportList = new List<ResortAirport>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    string sql = @"select agl3.airportid, agl3.geographylevel3id , gl3.name
from [ivdb].[lch].[dbo].[airportgeographylevel3] agl3
join [ivdb].[lch].[dbo].[geographylevel3] as gl3 on gl3.geographylevel3id = agl3.geographylevel3id";

                    conn.Open();
                    SqlCommand sqlCommand = new SqlCommand(sql, conn);
                    sqlCommand.CommandTimeout = 60000;

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        ResortAirport resortAirport = new ResortAirport();

                        resortAirport.AirportId = reader["AirportId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["AirportId"]);
                        resortAirport.ResortId = reader["GeographyLevel3ID"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["GeographyLevel3ID"]);
                        resortAirport.ResortName = reader["Name"].ToString();

                        resortAirportList.Add(resortAirport);
                    }
                    reader.Close();
                }
                DataTable resortAirportTable = Helpers.ToDataTable(resortAirportList);
                resortAirportTable.TableName = "ResortAirport";
                if (resortAirportTable.Rows.Count > 4000)
                {
                    Helpers.BulkAddDeleteFirst(resortAirportTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "ResortAirport not updated");
                }
                RemoveDuplicateResortAirports();
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateResortAirport", exception);
            }
        }

        private static void PopulateFacilitiesLists()
        {
            List<PropertyFacilityList> propertyFacilityLists = new List<PropertyFacilityList>();
            List<int> languageList = new List<int>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlLangCommand = new SqlCommand();
                    sqlLangCommand.CommandType = CommandType.Text;
                    sqlLangCommand.Connection = conn;

                    string langSelect = @"select distinct LanguageId from FacilityLanguage";

                    sqlLangCommand.CommandText = langSelect;

                    sqlLangCommand.CommandTimeout = 60000;

                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    SqlDataReader reader = sqlLangCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        int languageIdValue = reader["LanguageId"] == DBNull.Value
                            ? 1
                            : Convert.ToInt32(reader["LanguageId"]);
                        languageList.Add(languageIdValue);
                    }
                    reader.Close();

                    for (int i = 0; i < languageList.Count; i++)
                    {
                        SqlCommand sqlCommand = new SqlCommand();
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.Connection = conn;

                        string sql = @"SELECT DISTINCT Propfac.PropertyReferenceId AS [PropertyReferenceId],
STUFF(( SELECT ';' + SUB.FacilityDescription AS [text()]                        
FROM facilityLanguage SUB
WHERE facilityId IN (SELECT facilityId FROM propertyFacilities AS Pf wHERE Pf.PropertyReferenceId = propfac.PropertyReferenceId)
AND languageId = @LangId
FOR XML PATH('') 
), 1, 1, '' )
AS [FacilitiesList]
FROM PropertyFacilities propfac";

                        if (conn.State != ConnectionState.Open)
                            conn.Open();

                        sqlCommand.Parameters.Add("@LangId", SqlDbType.Int);
                        sqlCommand.Parameters["@LangId"].Value = languageList[i].ToString();

                        sqlCommand.CommandText = sql;
                        sqlCommand.CommandTimeout = 60000;

                        reader = sqlCommand.ExecuteReader();

                        while (reader.Read())
                        {
                            PropertyFacilityList propFacilityListItem = new PropertyFacilityList();
                            propFacilityListItem.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                                ? 1
                                : Convert.ToInt32(reader["PropertyReferenceId"]);
                            propFacilityListItem.LanguageId = languageList[i];
                            propFacilityListItem.FacilitiesList = reader["FacilitiesList"].ToString();

                            propertyFacilityLists.Add(propFacilityListItem);
                        }
                        reader.Close();
                    }
                }
                DataTable propertyFacilityListTable = Helpers.ToDataTable(propertyFacilityLists);
                propertyFacilityListTable.TableName = "PropertyFacilitiesList";
                if (propertyFacilityListTable.Rows.Count > 60000)
                {
                    Helpers.BulkAddDeleteFirst(propertyFacilityListTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "PropertyFacilitiesList not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateFacilitiesLists", exception);
            }
        }

        private static void PopulateImagesLists()
        {
            Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message", "Start PopulateImagesLists");
            Dictionary<int, PropertyImagesList> propertyImageListsDictionary = new Dictionary<int, PropertyImagesList>();

            int stepId = s_reportLogger.AddStep();
            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT distinct propim.PropertyReferenceId AS [PropertyReferenceId],
STUFF(( SELECT ';' + SUB.OtherImages AS [text()]                        
FROM PropertyImages SUB
WHERE
propim.PropertyReferenceId = SUB.PropertyReferenceId
FOR XML PATH('') 
), 1, 1, '' )
AS [ImageList]
FROM PropertyImages propim";

                    conn.Open();

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyImagesList propImList = new PropertyImagesList();
                        propImList.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceId"]);
                        propImList.ImageList = reader["ImageList"].ToString();
                        if (!propertyImageListsDictionary.ContainsKey(propImList.PropertyReferenceId))
                            propertyImageListsDictionary.Add(propImList.PropertyReferenceId, propImList);
                    }
                    reader.Close();
                }

                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message", "Number of images : " + propertyImageListsDictionary.Count);
                DataTable propertyImagesListTable = Helpers.ToDataTable(propertyImageListsDictionary.Values.ToList());
                propertyImagesListTable.TableName = "PropertyImageLists";
                if (propertyImagesListTable.Rows.Count > 100000)
                {
                    Helpers.BulkAddDeleteFirst(propertyImagesListTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "PropertyImageLists not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error", "Process->PopulateImagesLists", exception);
            }
        }

        private static void PopulateHighQualityImagesLists()
        {
            Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message",
                "Start PopulateHighQualityImagesLists");
            Dictionary<int, PropertyImageListHighQuality> propertyImageHighQualityListsDictionary =
                new Dictionary<int, PropertyImageListHighQuality>();

            int stepId = s_reportLogger.AddStep();

            try
            {
                using (SqlConnection conn = new SqlConnection(DataConnection.SqlConnCoreData))
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Connection = conn;

                    string sql = @"SELECT ap.PropertyReferenceID AS 'PropertyReferenceId',
STUFF((SELECT ';' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(li.ImageCNameUrl, OffsiteImageURL), '&amp;', ''), '&quot;', ''), '&lt;', ''), '&gt;', ''), ';', '') AS [text()]
FROM ivdb.LCB.dbo.cms_PropertyReference_LargeImages AS prli WITH(NOLOCK)
JOIN ivdb.LCB.dbo.CMS_LargeImages AS li WITH(NOLOCK) ON li.CMS_LargeImagesID = prli.CMS_LargeImagesID
WHERE cp.CMS_PropertyReferenceID = prli.CMS_PropertyReferenceID
ORDER BY li.Sequence ASC
FOR   XML PATH ('')), 1, 1, '') AS 'HighQualityImageList'
FROM (SELECT DISTINCT PropertyReferenceID FROM ivdb.LCB.dbo.PropertyReference WITH(NOLOCK) WHERE currentpropertyreference = 1 and GeographyLevel3ID <> 0) AS ap
JOIN ivdb.LCB.dbo.CMS_PropertyReference AS cp WITH(NOLOCK) ON ap.PropertyReferenceID = cp.SourceID
";

                    conn.Open();

                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandTimeout = 60000;

                    SqlDataReader reader = sqlCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        PropertyImageListHighQuality propImList = new PropertyImageListHighQuality();
                        propImList.PropertyReferenceId = reader["PropertyReferenceId"] == DBNull.Value
                            ? 0
                            : Convert.ToInt32(reader["PropertyReferenceId"]);
                        propImList.HighQualityImageList = reader["HighQualityImageList"].ToString();
                        if (String.IsNullOrEmpty(propImList.HighQualityImageList))
                            continue;
                        if (!propertyImageHighQualityListsDictionary.ContainsKey(propImList.PropertyReferenceId))
                            propertyImageHighQualityListsDictionary.Add(propImList.PropertyReferenceId, propImList);
                    }
                    reader.Close();
                }

                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Message",
                    "Number of images : " + propertyImageHighQualityListsDictionary.Count);
                DataTable propertyImagesHighQualityListTable =
                    Helpers.ToDataTable(propertyImageHighQualityListsDictionary.Values.ToList());
                propertyImagesHighQualityListTable.TableName = "PropertyHighQualityImageLists";
                if (propertyImagesHighQualityListTable.Rows.Count > 60000)
                {
                    Helpers.BulkAddDeleteFirst(propertyImagesHighQualityListTable);
                }
                else
                {
                    ReportLogger.AddMessage("CoreData", "PropertyUrls not updated");
                }
                s_reportLogger.EndStep(stepId);
            }
            catch (Exception exception)
            {
                s_reportLogger.EndStep(stepId, exception);
                Emailer.SendEmail("steven.jones@lowcostholidays.com", "CoreData  [SVRSQL4]: Error",
                    "Process->PopulateHighQualityImagesLists", exception);
            }
        }

    }
}
