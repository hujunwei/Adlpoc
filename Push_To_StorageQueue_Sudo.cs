// Create Storage Queue and Send Message
public void CreateQueueAndSendMessage()
{
    var queueName = "";
    var queueMessage = "";
    var queueHelper = new QueueHelper(StorageAccount, StorageKey);
    queueHelper.CreateQueue(queueName);
    queueHelper.PutMessage(queueName, queueMessage);       
}

// Queue Helper Class
public class QueueHelper : RESTHelper
{
    public QueueHelper(string storageAccount, string storageKey)
        : base("http://" + storageAccount + ".queue.core.windows.net/", storageAccount, storageKey)
    {
    }

    public bool CreateQueue(string queue)
    {
        return Retry(delegate
        {
            try
            {
                var response = CreateRESTRequest("PUT", queue).GetResponse() as HttpWebResponse;
                // ReSharper disable once PossibleNullReferenceException
                response.Close();
                return true;
            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ProtocolError &&
                    ex.Response != null &&
                    // ReSharper disable once PossibleNullReferenceException
                    (int) (ex.Response as HttpWebResponse).StatusCode == 409)
                    return false;

                throw;
            }
        });
    }

    public bool PutMessage(string queue, string messageBody)
    {
        return Retry(delegate
        {
            try
            {
                var messageBodyBytes = new UTF8Encoding().GetBytes(messageBody);
                var messageBodyBase64 = Convert.ToBase64String(messageBodyBytes);
                var message = "<QueueMessage><MessageText>" + messageBodyBase64 + "</MessageText></QueueMessage>";
                var response = CreateRESTRequest("POST", queue + "/messages", message).GetResponse() as HttpWebResponse;
                response.Close();
                return true;
            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ProtocolError &&
                    ex.Response != null &&
                    (int) (ex.Response as HttpWebResponse).StatusCode == 409)
                    return false;

                throw;
            }
        });
    }
}