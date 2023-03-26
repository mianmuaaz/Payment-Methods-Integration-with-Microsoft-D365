using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Core.Blob;
using VSI.CloudPlatform.Core.Common;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Core.Telemetry;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Jobs;

namespace FunctionApp.RCK.D365.OrderSend
{
    public static class OrderSend
    {
        private static readonly bool _excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);
        private static readonly string instrumentationKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static readonly int d365ConfigCacheExpireTimeout = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("D365ConfigCacheExpireTimeout"), 60);
        private static IBlob blob;
        private static ICloudDb cloudDb;

        static OrderSend()
        {
            blob = new AzureBlob(Environment.GetEnvironmentVariable("StorageConnectionString"));
            cloudDb = new CosmosCloudDb(Environment.GetEnvironmentVariable("CosmosConnectionString"));

            BindingRedirectApplicationHelper.Startup();
        }

        [FunctionName("FUNC_DAX365OrderSend")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "POST", Route = null)] HttpRequestMessage req)
        {
            IOperationHolder<RequestTelemetry> operation = null;
            TelemetryClient telemetryClient = null;

            var startDate = DateTime.Now;
           
            try
            {
                //Function Deploy
                var request = await req.Content.ReadAsStringAsync();
                var functionParams = JsonConvert.DeserializeObject<FunctionParams>(request);

                var instanceKey = $"{functionParams.TransactionName}_{functionParams.PartnerShipId}_{functionParams.TransactionStep}";

                telemetryClient = TelemetryFactory.GetInstance(instanceKey, instrumentationKey, _excludeDependency);
                operation = telemetryClient.StartOperation<RequestTelemetry>(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, Guid.NewGuid().ToString());

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Starting...");
                telemetryClient.AddDefaultProperties(functionParams);
                

                var salesOrderParams = FunctionHelper.GetSalesOrderRequestParams(functionParams);
                //Function Helper 

                if (functionParams.IsScheduled)
                {
                    await FunctionHelper.ProcessDataAsync(salesOrderParams, telemetryClient, functionParams, startDate, cloudDb, blob);
                }
                else
                {
                    await FunctionHelper.ManualProcessAsync(functionParams, telemetryClient, cloudDb, blob);
                }
                

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, "Finished.");

                return req.CreateResponse(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                var depthException = CommonUtility.GetDepthInnerException(ex);

                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(depthException);
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, "error occured" + ex);
                }

                throw;
            }
            finally
            {
                telemetryClient.StopOperation(operation);
            }
        }
    }
}