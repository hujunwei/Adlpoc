// Create Storage Queue and Send Message
// If queue exists, calling this will not create a new queue, instead, use same queue.
public void CreateQueueAndSendMessage(List<FabricUsageModel> table)
{
    var storageUrl = "https//<storage_account_name>.queue.core.windows.net/";
    var storageKey = "<storage_account_key>";
    var queueName = "TestQueue";
    // Serialize Table as string
    var queueMessageInJson = JsonConvert.SerializeObject(aList);;

    // Create a storage queue with queueName
    CreateRESTRequest("PUT", queueName, storageUrl, storageKey);
    
    // Add serialized table as message body
    var messageBodyBytes = new UTF8Encoding().GetBytes(queueMessageInJson);
    var messageBodyBase64 = Convert.ToBase64String(messageBodyBytes);
    var message = "<QueueMessage><MessageText>" + messageBodyBase64 + "</MessageText></QueueMessage>";
    
    // Push table to queue
    CreateRESTRequest("POST", queueName + "/messages", message, storageUrl, storageKey);   
    
    return; 
}

// Listen to Storage Queue and get message body
public void Execute([QueueTrigger("TestQueue")]string message, TextWriter log)
{
    var table = JsonConvert.DeserializeObject<List<FabricUsageModel>>(message);

    foreach (var row in table) {
        SqlClient.WriteToSQL(row);
    }

    return;
}

