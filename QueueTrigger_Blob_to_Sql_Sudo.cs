// Listen to Storage Queue and get message
public override async void Execute([QueueTrigger(QueueName)]string message, TextWriter log)
{
    await LoadBlobAndCallDataLoader(message);
}
 
// Load Blob and write to SQL
public async Task LoadBlobAndCallDataLoader(string message)
{
    DateTime loadDate;
    DateTime.TryParse(message, out loadDate);
    var blobModels = await BlobClient.DownloadBlobAsync<T>(EntityName, loadDate, false, false);
    await InsertOrUpdateDataAsync(blobModels.ToList(), loadDate)
}

// Convert Flat Data in Blob to dimension-fact model and bulkupdate to SQL
protected override async Task<bool> InsertOrUpdateDataAsync(IReadOnlyList<FabricUsageByCluster> clusterFabricUsage, DateTime loadDate)
{
    var dataTable = new FT_Cluster_Fabric_Usage_AvZone_Table();
    var avZoneKeyDictionary = KeyFinder.GetAvailabilityZoneKeyDictionary();
    var resourceKeyDictionary = KeyFinder.GetResourceKeyDictionary();
    var clusterKeyDictionary = KeyFinder.GetClusterKeyDictionary();
    var reportDate = loadDate.Date;
    var dateKey = await KeyFinder.GetDateKey(reportDate);
    foreach (var usage in clusterFabricUsage)
    {
        var avZoneKey = await KeyFinder.GetAvailabilityZoneKey(usage.AvailabilityZone, usage.Geo, usage.Region, avZoneKeyDictionary);
        var resourceKey = KeyFinder.GetResourceKey(usage.ClusterType, resourceKeyDictionary);
        var clusterKey = await KeyFinder.GetClusterKey(usage.Cluster, usage.ClusterType, usage.SKUName, usage.AvailabilityZone,
            usage.Region, usage.Geo, clusterKeyDictionary);
        var fabricUsageRow = usage.CreateFactFromSelf();
        fabricUsageRow.Cluster_Key = clusterKey;
        fabricUsageRow.AvZone_Key = avZoneKey;
        fabricUsageRow.Date_Key = dateKey;
        fabricUsageRow.Resource_Key = resourceKey;
        dataTable.AddRow(fabricUsageRow);
    }
    var rowCountThreshold = ConfigurationHelper.GetRowCountThreshold($"{DataLoaderName}RowCountThreshold");
    return SqlDbClient.BulkUpdate(dataTable.Get(), TableName, dateKey, rowCountThreshold);
}

// SQL Bulk Update function
public bool BulkUpdate(DataTable table, string destTableName, int dateKey, float rowCountThreshold, ISqlDbClientTableFilter filter = null)
{
    using (var connection = new SqlConnection(_connString))
    {
        connection.Open();
        using (var transaction = connection.BeginTransaction(IsolationLevel.Snapshot))
        {
            try
            {
                var numRowsExist = GetRowCountForTable(destTableName, filter, connection, transaction);
                if (!IsValidToBulkUpdate(numRowsExist, table.Rows.Count, rowCountThreshold))
                {
                    return false;
                }
                BulkDelete(destTableName, filter, connection, transaction);
                BulkInsert(table, destTableName, connection, transaction);
                transaction.Commit();
                return true;
            }
            catch (Exception ex)
            {
               transaction.Rollback();
               return false;
            }
        }
    }
}
 
private void BulkInsert(
    DataTable table,
    string destTableName,
    SqlConnection connection,
    SqlTransaction transaction,
    SqlBulkCopyOptions options = SqlBulkCopyOptions.CheckConstraints,
    int bulkInsertTimeOut = 60)
{
    using (var bulkCopy = new SqlBulkCopy(connection, options, transaction))
    {
        bulkCopy.BulkCopyTimeout = bulkInsertTimeOut;
        bulkCopy.DestinationTableName = destTableName;
        bulkCopy.WriteToServer(table);
    }
}

private void BulkDelete(
    string tableName,
    ISqlDbClientTableFilter filter,
    SqlConnection connection,
    SqlTransaction transaction)
{

    var deleteQueryString = $"DELETE FROM {tableName} WHERE {filter.FilterClause}";
    using (var command = new SqlCommand(deleteQueryString, connection))
    {
        filter.AddParameters(command.Parameters);
        command.Transaction = transaction;
        command.CommandType = CommandType.Text;
        var numRowDeleted = command.ExecuteNonQuery();
        _logger.Info($"Deleted {numRowDeleted} in {tableName} with filter {filter.LogInfo}", "SQLDbClient::BulkDelete");
    }
}