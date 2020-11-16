using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace CheckFiles
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            var config = new ConfigurationBuilder()
              .AddEnvironmentVariables()
              .Build();

            string keyVaultUri = config["KeyVaultUri"];
            string emailServiceUrl = config["EmailServiceUrl"];
            string emailJwtToken = config["EmailJwtToken"]; //Secret
            string monitoringUrl = config["MonitoringUrl"];
            string monitoringApiKey = config["MonitoringApiKey"]; //Secret
            string unhandledEmailTemplateId = config["UnhandledEmailTemplateId"]; //Secret
            string sbConnectionString = config["processed-files-topic-ListenKey"]; //Secret

            string emailJwtTokenSecretValue = emailJwtToken;
            string monitoringApiKeySecretValue = monitoringApiKey;
            string unhandledEmailTemplateIdSecretValue = unhandledEmailTemplateId;
            string sbConnectionStringSecretValue = sbConnectionString;
            //try
            //{
            //    var client = new SecretClient(new Uri(keyVaultUri), new DefaultAzureCredential());
            //    KeyVaultSecret emailJwtTokenSecret = await client.GetSecretAsync(emailJwtToken);
            //    emailJwtTokenSecretValue = emailJwtTokenSecret.Value;
            //    KeyVaultSecret monitoringApiKeySecret = await client.GetSecretAsync(monitoringApiKey);
            //    monitoringApiKeySecretValue = monitoringApiKeySecret.Value;
            //    KeyVaultSecret unhandledEmailTemplateIdSecret = await client.GetSecretAsync(unhandledEmailTemplateId);
            //    unhandledEmailTemplateIdSecretValue = unhandledEmailTemplateIdSecret.Value;
            //    KeyVaultSecret sbConnectionStringSecret = await client.GetSecretAsync(sbConnectionString);
            //    sbConnectionStringSecretValue = sbConnectionStringSecret.Value;
            //}
            //catch (Exception e)
            //{
            //    throw new Exception($"Failed to retreive secrets from KeyVault: {e.Message}");
            //}

            int[] databaseTenantIds = { 1, 2, 3, 4, 5, 6, 7 }; // These values have matching Ids in the database
            List<File> unhandledFiles = new List<File>();
            List<File> handledFiles = new List<File>();
            foreach (var id in databaseTenantIds)
            {
                try
                {
                    MessageReceiver messageReceiver = new MessageReceiver(sbConnectionStringSecretValue,
                        EntityNameHelper.FormatSubscriptionPath("processed-files-topic", $"processed-files-queue-{id}"),
                        ReceiveMode.PeekLock);

                    var message = await messageReceiver.ReceiveAsync(TimeSpan.FromSeconds(3)); // Picks up a message from the Service bus subscription
                    if (message != null)
                    {
                        string sbmessage = Encoding.UTF8.GetString(message.Body);
                        log.LogDebug($"Received data from processed-files-queue-{id}: {sbmessage}");
                        List<File> files = files = JsonConvert.DeserializeObject<List<File>>(sbmessage);
                        List<File> tempUnhandledFiles = new List<File>();
                        foreach (var item in files)
                        {
                            if (!string.IsNullOrEmpty(item.ErrorMessage))
                            {
                                unhandledFiles.Add(item);
                                tempUnhandledFiles.Add(item);
                            }
                            else
                            {
                                handledFiles.Add(item);
                            }
                        }
                        SendEmailData(tempUnhandledFiles, emailJwtTokenSecretValue, emailServiceUrl, unhandledEmailTemplateIdSecretValue);
                        SendMonitorData(tempUnhandledFiles, monitoringUrl, monitoringApiKeySecretValue);
                        await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
                    }
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed: {e.Message}");
                }
            }

            log.LogInformation($"Number of successful files: {handledFiles.Count}");
            log.LogInformation($"Number of unhandled files: {unhandledFiles.Count}");
            log.LogDebug($"Failed files: {JsonConvert.SerializeObject(unhandledFiles)}");
        }

        private static void SendEmailData(List<File> files, string emailJwtToken, string emailServiceUrl, string unhandledEmailTemplateId)
        {
            try
            {
                var emailFiles = new List<EmailFile>();
                foreach (File file in files)
                {
                    EmailFile emailFile = new EmailFile()
                    {
                        Date = file.Timestamp.ToString(),
                        UploadedBy = file.UploadedBy,
                        Timestamp = file.Timestamp.ToString("yyyy-mm-dd HH:mm:ss"),
                        ErrorMessage = file.ErrorMessage
                    };
                    emailFiles.Add(emailFile);
                }

                using var httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", emailJwtToken);
                var content = new StringContent(JsonConvert.SerializeObject(emailFiles), Encoding.UTF8, "application/json");
                var result = httpClient.PostAsync($"{emailServiceUrl}/{unhandledEmailTemplateId}", content).Result;
                if (!result.IsSuccessStatusCode)
                {
                    throw new Exception($"Http request to {emailServiceUrl} result: {(int)result.StatusCode} {result.StatusCode}");
                }
            }
            catch (Exception e)
            {
                throw new Exception($"SendEmailData method: {e.Message}");
            }
        }

        private static void SendMonitorData(List<File> files, string monitoringUrl, string monitoringApiKey)
        {
            try
            {
                using var httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Add("api-key", monitoringApiKey);
                var content = new StringContent(JsonConvert.SerializeObject(files), Encoding.UTF8, "application/json");
                var result = httpClient.PostAsync($"{monitoringUrl}", content).Result;
                if (!result.IsSuccessStatusCode)
                {
                    throw new Exception($"Http request to {monitoringUrl} result: {(int)result.StatusCode} {result.StatusCode}");
                }
            }
            catch (Exception e)
            {
                throw new Exception($"SendMonitorData method: {e.Message}");
            }
        }

        private class File
        {
            public int FileId { get; set; }
            public string UploadedBy { get; set; }
            public string ErrorMessage { get; set; }
            public DateTime Timestamp { get; set; }
        }

        private class EmailFile
        {
            public string Date { get; set; }
            public string UploadedBy { get; set; }
            public string Timestamp { get; set; }
            public string ErrorMessage { get; set; }
        }
    }
}
