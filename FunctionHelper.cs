using Microsoft.ApplicationInsights;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using RCK.CloudPlatform.AXD365;
using RCK.CloudPlatform.Common;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model.Customer;
using RCK.CloudPlatform.Model.ERP;
using RCK.CloudPlatform.Model.SalesOrder;
using RCK.CloudPlatform.Model.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Enums;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Core.Db;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Common;
using VSI.CloudPlatform.Model.Jobs;
using VSI.Contants;
using VSI.ErrorProcessor;
using VSI.Model;
using System.IO;

namespace FunctionApp.RCK.D365.OrderSend
{
    public class FunctionHelper
    {
        public static async Task ManualProcessAsync(FunctionParams functionParams, TelemetryClient telemetryClient, ICloudDb cloudDb, IBlob blob)
        {
            try
            {
                var startDate = DateTime.Now;

                var archiveBlobContainer = Environment.GetEnvironmentVariable("BlobArchiveContainer");

                var requestParams = GetSalesOrderRequestParams(functionParams);


                var stage = JsonConvert.DeserializeObject<EDI_STAGING>(File.ReadAllText(@"D:\\stage.txt"));



                var originalMessage = GetFileFromPreviousStep(stage, blob);

                var salesOrder = JsonConvert.DeserializeObject<RCKSalesOrder>(originalMessage);

                ProcessSalesOrder(salesOrder, requestParams, telemetryClient, stage, startDate, cloudDb, blob);

            }
            catch (Exception ex)
            {
                telemetryClient.TrackException(ex);

                var depthException = CommonUtility.GetDepthInnerException(ex);
                var error = ErrorProcessorUtility.GetLogErrorobject(0, depthException?.Message, Global.ErrorType.Transport, 0, Global.ErrorCodes.InboundTransportationError, "1", functionParams.partnerShip);

                ErrorDataDbHelper.AddErrorData(error, cloudDb);
                throw;
            }
        }

        public static string GetFileFromPreviousStep(EDI_STAGING stageData, IBlob blob)
        {
            Steps lastStep = null;
            var fileContent = string.Empty;
            if (stageData.StepsDetails.Any())
                lastStep = stageData.StepsDetails.OrderBy(x => x.StepOrder).LastOrDefault();
            if (lastStep != null && lastStep.Status == MessageStatus.COMPLETED)
            {
                fileContent = blob.ReadFileContentAsync(lastStep.FileUrl).Result;
            }
            return fileContent;
        }
        internal static SalesOrderParams GetSalesOrderRequestParams(FunctionParams functionParams)
        {
            var functionSettings = functionParams.Settings;
            var securityParams = JsonConvert.DeserializeObject<HttpSecurity>(functionSettings.GetValue("D365SecuritySettings"));

            return new SalesOrderParams
            {
                D365RetailConnectivity = new D365RetailConnectivity
                {
                    AzAD = securityParams.Settings.GetValue("ADTenant"),
                    ClientId = securityParams.Settings.GetValue("ADClientAppId"),
                    ClientSeceret = securityParams.Settings.GetValue("ADClientAppSecret"),
                    D365Uri = securityParams.Settings.GetValue("ADResource"),
                    RetailServerUri = securityParams.Settings.GetValue("RCSUResource"),
                    OperatingUnitNumber = securityParams.Settings.GetValue("OUN")
                },
                MagentoSalesResponseServiceUri = functionSettings.GetValue("ECOMOrderResponseApi"),
                MagentoCustResponseServiceUri = functionSettings.GetValue("ECOMCustomerResponseApi"),
                MagentoApiToken = functionSettings.GetValue("ECOMApiToken"),
                D365StaticCustomers = functionSettings.GetValue("D365StaticCustomers"),
                ServicebusConnectionString = functionSettings.GetValue("ServicebusConnectionString"),
                SourceTopic = functionSettings.GetValue("SourceTopic"),
                SourceTopicSubscription = functionSettings.GetValue("SourceTopicSubscription"),
                ArchiveBlobContainer = Environment.GetEnvironmentVariable("ArchiveBlobContainer"),
                MessageBatchSize = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessageBatchSize"), 100),
                MessagePumpTimeout = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessagePumpTimeout"), 120),
                MessageReceiverTimeout = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessageReceiverTimeout"), 20),
                MaxDegreeOfParallelism = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MaxDegreeOfParallelism"), 10),
                ProcessName = functionParams.TransactionStep,
                WorkfowName = functionParams.TransactionName,
                FunctionParams = functionParams
            };
        }

        internal async static Task ProcessDataAsync(SalesOrderParams requestParams, TelemetryClient telemetryClient, FunctionParams functionParams, DateTime startDate, ICloudDb cloudDb, IBlob blob)
        {
            var activeMessageCount = FunctionUtilities.GetMessageCount(requestParams.ServicebusConnectionString, requestParams.SourceTopic, requestParams.SourceTopicSubscription);
            var threadCount = FunctionUtilities.GetThreads(requestParams.MaxDegreeOfParallelism, activeMessageCount);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Thread Count {threadCount}, Message Count {activeMessageCount}");

            if (threadCount == 0)
            {
                return;
            }

            var entityPath = EntityNameHelper.FormatSubscriptionPath(requestParams.SourceTopic, requestParams.SourceTopicSubscription);
            var messageReceiver = new MessageReceiver(requestParams.ServicebusConnectionString, entityPath, ReceiveMode.PeekLock);
            var cancelationToken = new CancellationTokenSource(TimeSpan.FromSeconds(requestParams.MessagePumpTimeout));

            while (!cancelationToken.IsCancellationRequested)
            {
                var messages = await messageReceiver.ReceiveAsync(requestParams.MessageBatchSize, TimeSpan.FromSeconds(requestParams.MessageReceiverTimeout));
                if (messages == null || messages.Count() == 0)
                {
                    break;
                }

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Received batch count {messages.Count()}.");

                Parallel.ForEach(messages, new ParallelOptions { MaxDegreeOfParallelism = Convert.ToInt32(threadCount) }, message =>
                {
                    var messageProperties = message.UserProperties;
                    var stage = JsonConvert.DeserializeObject<EDI_STAGING>(Encoding.Default.GetString(message.Body));

                    try
                    {
                        var blobUri = stage.StepsDetails.Where(key => key.StepName != functionParams.TransactionStep && key.Status != MessageStatus.ERROR && key.StepName != "Load" && key.StepName != "Receive")
                                                        .OrderByDescending(key => key.StepOrder).FirstOrDefault()?.FileUrl;

                        var originalMessage = CommonUtility.GetFromBlobAsync(blob, blobUri).GetAwaiter().GetResult();

                        var salesOrder = JsonConvert.DeserializeObject<RCKSalesOrder>(originalMessage);

                        ProcessSalesOrder(salesOrder, requestParams, telemetryClient, stage, startDate, cloudDb, blob);

                        messageReceiver.CompleteAsync(message.SystemProperties.LockToken).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        var depthException = CommonUtility.GetDepthInnerException(ex);

                        CommonUtility.LogErrorStageMessage(cloudDb, stage, functionParams, startDate, depthException, true);

                        telemetryClient.TrackException(depthException);
                    }
                });
            }

            await messageReceiver.CloseAsync();
        }

        private static void ProcessSalesOrder(RCKSalesOrder salesOrder, SalesOrderParams salesOrderParams, TelemetryClient telemetryClient, EDI_STAGING stage, DateTime startDate, ICloudDb cloudDb, IBlob blob)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Connecting to Retail Server store {salesOrder.Order.OperatingUnitNumber}...");

            salesOrderParams.D365RetailConnectivity.OperatingUnitNumber = salesOrder.Order.OperatingUnitNumber;

            var retailContext = RetailConnectivityController.ConnectToRetailServerAsync(salesOrderParams.D365RetailConnectivity).GetAwaiter().GetResult();

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, "Transaction Id: {stage.Transaction_Id}, Connected with Retail Server.");

            if (salesOrder.Customer.IsGuestCustomer == "1")
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Received Guest customer.");
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Creating customer {salesOrder.Customer.EcomCustomerId} in D365.");
                var customer = GetCosmosCustomer(cloudDb, salesOrder.Customer.Email, salesOrder.Order.StoreId);
                if (customer != null)
                {
                    salesOrder.Order.CustomerId = customer.ErpCustomerAccountNumber;

                }
                else
                {

                    var customerResponse = CreateCustomerToD365(salesOrder, salesOrderParams, telemetryClient, stage, cloudDb, retailContext);
                    salesOrder.Order.CustomerId = customerResponse.AccountNumber;
                    if (customerResponse.Addresses.Any(a => a.AddressTypeValue == 2))
                    {
                        var shippingAddressRecId = customerResponse.Addresses.FirstOrDefault(a => a.AddressTypeValue == 2).RecordId;
                        salesOrder.Order.ShippingAddress.RecordId = shippingAddressRecId;
                        foreach (var line in salesOrder.Order.SalesLines)
                        {
                            if (line.ShippingAddress != null && line.ShippingAddress?.AddressTypeValue == 2)
                            {
                                line.ShippingAddress.RecordId = shippingAddressRecId;
                            }
                        }
                    }
                }
            }
            else
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Checking customer {salesOrder.Customer.Email} existance in COSMOS DB.");

                var customer = GetCosmosCustomer(cloudDb, salesOrder.Customer.Email, salesOrder.Order.StoreId);

                // Check 1: If customer exist on CL then use it

                if (customer != null)
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Customer {salesOrder.Customer.EcomCustomerId} found in COSMOS with Erpkey {customer.ErpCustomerId}.");

                    salesOrder.Order.CustomerId = customer.ErpCustomerAccountNumber;

                    // Check 2: If new customer address added

                    var addresses = salesOrder.Customer.Addresses.Where(a => a.RecordId == 0).ToList();
                    var d365StaticCustomers = !string.IsNullOrWhiteSpace(salesOrderParams.D365StaticCustomers) ? salesOrderParams.D365StaticCustomers.Split(',') : new string[] { };

                    if (addresses.Count > 0 && !d365StaticCustomers.Contains(customer.ErpCustomerAccountNumber))
                    {
                        var customerByAccountNumber = CustomerController.GetByAccountNumberAsync(retailContext, customer.ErpCustomerAccountNumber).GetAwaiter().GetResult();

                        if (customerByAccountNumber == null)
                        {
                            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Customer {customer.ErpCustomerAccountNumber} exist in CL cache DB but not exist in D365!");

                            var customerResponse = CreateCustomerToD365(salesOrder, salesOrderParams, telemetryClient, stage, cloudDb, retailContext);

                            CosmosHelper.Delete(cloudDb, customer.id, customer.Entity, RCKDbTables.IntegrationData);

                            salesOrder.Order.CustomerId = customerResponse.AccountNumber;

                            if (addresses.Any(a => a.AddressTypeValue == 2))
                            {
                                var shippingAddressRecId = customerResponse.Addresses.FirstOrDefault(a => a.AddressTypeValue == 2).RecordId;

                                salesOrder.Order.ShippingAddress.RecordId = shippingAddressRecId;
                                foreach (var line in salesOrder.Order.SalesLines)
                                {
                                    if (line.ShippingAddress != null && line.ShippingAddress?.AddressTypeValue == 2)
                                    {
                                        line.ShippingAddress.RecordId = shippingAddressRecId;
                                    }
                                }
                            }
                        }
                        else
                        {
                            if (!addresses.Select(a => a.State).Any(a => a.Contains("PR")))
                            {
                                customerByAccountNumber.EcomCustomerId = salesOrder.Customer.EcomCustomerId;

                                var addressesResponse = CustomerController.CreateAddressesAsync(retailContext, customerByAccountNumber, addresses, telemetryClient, RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, stage.Transaction_Id).GetAwaiter().GetResult();

                                #region Saving refrences in COSMOS DB & Sending to ECOM

                                var properAddresses = addressesResponse?.Where(a => !string.IsNullOrEmpty(a.EcomAddressId))?.ToList();
                                if (properAddresses != null && properAddresses.Count > 0)
                                {
                                    var customerWithProperaddresses = JsonConvert.DeserializeObject<ErpCustomer>(JsonConvert.SerializeObject(customerByAccountNumber));
                                    customerWithProperaddresses.Addresses.Clear();
                                    customerWithProperaddresses.Addresses = properAddresses;

                                    UpdateCustomerReferenceInCosmosDb(customerWithProperaddresses, cloudDb, customer);

                                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Sending customer reference to ECOM.");

                                    CustomerController.SendCustomerResponseToEcom(customerWithProperaddresses, salesOrder.Customer, telemetryClient, salesOrderParams.MagentoCustResponseServiceUri, salesOrderParams.MagentoApiToken, stage.Transaction_Id);
                                }

                                #endregion

                                salesOrder.Order.CustomerId = customer.ErpCustomerAccountNumber;

                                if (addresses.Any(a => a.AddressTypeValue == 2))
                                {
                                    var shippingAddressRecId = addressesResponse.FirstOrDefault(a => a.AddressTypeValue == 2).RecordId;

                                    salesOrder.Order.ShippingAddress.RecordId = shippingAddressRecId;
                                    foreach (var line in salesOrder.Order.SalesLines)
                                    {
                                        if (line.ShippingAddress != null && line.ShippingAddress?.AddressTypeValue == 2)
                                        {
                                            line.ShippingAddress.RecordId = shippingAddressRecId;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        salesOrder.Order.CustomerId = customer.ErpCustomerAccountNumber;
                    }
                }
                else
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Creating customer {salesOrder.Customer.EcomCustomerId} in D365.");

                    var customerResponse = CreateCustomerToD365(salesOrder, salesOrderParams, telemetryClient, stage, cloudDb, retailContext);

                    salesOrder.Order.CustomerId = customerResponse.AccountNumber;

                    if (customerResponse.Addresses.Any(a => a.AddressTypeValue == 2))
                    {
                        var shippingAddressRecId = customerResponse.Addresses.FirstOrDefault(a => a.AddressTypeValue == 2).RecordId;

                        salesOrder.Order.ShippingAddress.RecordId = shippingAddressRecId;
                        foreach (var line in salesOrder.Order.SalesLines)
                        {
                            if (line.ShippingAddress != null && line.ShippingAddress?.AddressTypeValue == 2)
                            {
                                line.ShippingAddress.RecordId = shippingAddressRecId;
                            }
                        }
                    }
                }
            }

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Creating Sales order in D365 Channel DB.");

            // Create sales order in channel DB, Channel JOB will process Sales order to HQ
            // Changed in Line Attributes for Configurator Product
            var salesId = SalesOrderController.Create(retailContext, salesOrder.Order, salesOrder.HeaderExtensionProperties, salesOrder.HeaderAttributes, salesOrder.CardPaymentProperties,
                salesOrder.AuthPaymentProperties, telemetryClient, salesOrderParams.MagentoSalesResponseServiceUri,
                salesOrderParams.MagentoApiToken, RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, bool.Parse(salesOrder.IsAdyenOrder), bool.Parse(salesOrder.IsPaypalOrder), bool.Parse(salesOrder.IsApplePayOrder), salesOrder.AdyaneConnectorName, bool.Parse(salesOrder.IsOnAccountOrder), bool.Parse(salesOrder.IsiGlobalCreditCard), bool.Parse(salesOrder.IsCheckMoOrder), bool.Parse(salesOrder.IsGiveXOrder), bool.Parse(salesOrder.IsAmazonOrder), salesOrder.OnAccountTenderTypeId, salesOrder.iGlobalTenderTypeId, salesOrder.checkMoTenderTypeId, salesOrder.GiveXTenderTypeId);

            IntegrationManager.SaveIntegrationKey(cloudDb, Entities.SaleOrder, salesId, salesOrder.Order.EcomSalesId);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Uploading original message to blob storage.");

            var blobPath = CommonUtility.UploadOutboundDataFilesOnBlobAsync(salesOrderParams.FunctionParams, blob, $"Sales Order successfully processed to {salesOrderParams.D365RetailConnectivity.OperatingUnitNumber} channel with transaction {salesId}.", stage.Transaction_Id, salesOrderParams.ArchiveBlobContainer).GetAwaiter().GetResult();

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Uploaded original message to blob storage.");
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Pushing Stage Success Message.");
            stage.ISAControlNum = salesOrder.Order.ChannelReferenceId;
            stage.KeyData1 = salesOrder.Order.EcomSalesId;
            stage.KeyData2 = salesId;
            CommonUtility.LogSuccessStageMessage(cloudDb, stage, salesOrderParams.FunctionParams, startDate, OverallStatus.IMPORTED, MessageStatus.COMPLETED, blobPath);


        }

        private static ErpCustomer CreateCustomerToD365(RCKSalesOrder salesOrder, SalesOrderParams salesOrderParams, TelemetryClient telemetryClient, EDI_STAGING stage, ICloudDb cloudDb, D365RetailServerContext retailContext)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Creating customer {salesOrder.Customer.EcomCustomerId} to D365!");
            if (salesOrder.Customer.IsGuestCustomer == "1")
            {
                salesOrder.Customer.EcomCustomerId = "1";
            }
            var customerResponse = CustomerController.CreateAsync(retailContext, salesOrder.Customer, telemetryClient, RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, stage.Transaction_Id).GetAwaiter().GetResult();

            //if (salesOrder.Customer.IsGuestCustomer == "1" && !salesOrder.Customer.Addresses.Select(a => a.State).Any(a => a.Contains("PR")))
            //{
            //    customerResponse.Addresses = salesOrder.Customer.Addresses;
            //}
            if (!salesOrder.Customer.Addresses.Select(a => a.State).Any(a => a.Contains("PR")))
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Creating customer {salesOrder.Customer.EcomCustomerId} addresses to D365!");

                var addressResponse = CustomerController.CreateAddressesAsync(retailContext, customerResponse, salesOrder.Customer.Addresses.ToList(), telemetryClient, RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, stage.Transaction_Id).GetAwaiter().GetResult();

                customerResponse.Addresses.Clear();
                customerResponse.Addresses = addressResponse;

                #region Saving references in COSMOS DB & Sending to ECOM

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Saving customer reference in COSMOS DB.");

                var customerWithProperaddresses = JsonConvert.DeserializeObject<ErpCustomer>(JsonConvert.SerializeObject(customerResponse));
                if (salesOrder.Customer.IsGuestCustomer == "1")
                {
                    var properAddresses = addressResponse?.Where(a => string.IsNullOrEmpty(a.EcomAddressId))?.ToList();
                    customerWithProperaddresses.Addresses.Clear();
                    customerWithProperaddresses.Addresses = properAddresses;
                }
                else
                {
                    var properAddresses = addressResponse?.Where(a => !string.IsNullOrEmpty(a.EcomAddressId))?.ToList();

                    if (properAddresses != null && properAddresses.Count > 0)
                    {
                        customerWithProperaddresses.Addresses.Clear();
                        customerWithProperaddresses.Addresses = properAddresses;
                    }
                    else
                    {
                        customerWithProperaddresses.Addresses = new List<ErpAddress>();
                    }
                }

                customerWithProperaddresses.StoreId = salesOrder.Order.StoreId;
                CustomerController.SaveCustomerReferenceInCosmosDb(customerWithProperaddresses, cloudDb);

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_ORDER_SEND, $"Transaction Id: {stage.Transaction_Id}, Sending customer reference to ECOM.");
                if (salesOrder.Customer.IsGuestCustomer == "1")
                {
                    return customerResponse;
                }

                CustomerController.SendCustomerResponseToEcom(customerWithProperaddresses, salesOrder.Customer, telemetryClient, salesOrderParams.MagentoCustResponseServiceUri, salesOrderParams.MagentoApiToken, stage.Transaction_Id);
                #endregion

                return customerResponse;
            }

            return customerResponse;
        }

        private static void UpdateCustomerReferenceInCosmosDb(ErpCustomer erpCustomer, ICloudDb cloudDb, CosmosCustomer cosmosCustomer)
        {
            var addresses = new List<CosmosCustomerAddress>();

            foreach (var address in erpCustomer.Addresses)
            {
                addresses.Add(new CosmosCustomerAddress
                {
                    address_id = address.EcomAddressId,
                    erp_address_id = address.RecordId.ToString(),
                    billing = address.AddressTypeValue == 1,
                    shipping = address.AddressTypeValue == 2
                });
            }

            var diffExistingCosmosAddresses = cosmosCustomer.Adddresses.Where(ad => !erpCustomer.Addresses.Any(a => ad.address_id == a.EcomAddressId)).ToList();

            if (diffExistingCosmosAddresses.Count > 0)
            {
                addresses.AddRange(diffExistingCosmosAddresses);
            }

            cosmosCustomer.Adddresses = addresses;

            CosmosHelper.Update(cloudDb, cosmosCustomer, RCKDbTables.IntegrationData);
        }

        private static CosmosCustomer GetCosmosCustomer(ICloudDb cloudDb, string email, string storeId)
        {
            var result = cloudDb.ExecuteQueryForRawJson(RCKDbTables.IntegrationData, $"SELECT * FROM c WHERE c.Email='{email}' AND c.Entity = '{Entities.Customer}' AND c.StoreId = {storeId}");

            if (!string.IsNullOrWhiteSpace(result))
            {
                var customer = JsonConvert.DeserializeObject<CosmosCustomer>(result);

                return customer;
            }

            return null;
        }
    }
}