using Microsoft.ApplicationInsights;
using Microsoft.Dynamics.Commerce.RetailProxy;
using Microsoft.Dynamics.Retail.PaymentSDK.Portable;
using Microsoft.Dynamics.Retail.PaymentSDK.Portable.Constants;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.Model;
using RCK.CloudPlatform.Model.Cosmos;
using RCK.CloudPlatform.Model.ERP;
using RCK.CloudPlatform.Model.SalesOrder;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Common;
using VSI_RT = VSI.Commerce.RetailProxy;

namespace RCK.CloudPlatform.D365
{
    public class SalesOrderController
    {
        #region SALES ORDER

        public static string Create(D365RetailServerContext retailContext, ErpSalesOrder erpSalesOrder, List<KeyValue> headerExtensionProperties, List<KeyValue> headerAttributes,
            List<ErpPaymentProperties> cardPaymentProperties, List<ErpPaymentProperties> authPaymentProperties, TelemetryClient telemetryClient,
            string responseApi, string responseApiToken, string functionName, bool isAdyenOrder, bool isPaypalOrder, bool isApplePayOrder, string connectorName, bool isOnAccountOrder, bool isiGlobalCreditCard, bool isCheckMoOrder, bool isGiveXOrder, bool isAmazonOrder, int onAccountTenderTypeId, int iGlobalTenderTypeId, int checkMoTenderTypeId, int GiveXTenderTypeId)
        {
            try
            {
                var manager = retailContext.FactoryManager.GetManager<VSI_RT.ISalesOrderManager>();

                var erpSalesOrderString = JsonConvert.SerializeObject(erpSalesOrder);

                var salesOrder = JsonConvert.DeserializeObject<SalesOrder>(erpSalesOrderString);

                InitNullPrimitiveCollection(salesOrder);
                AddShippingAddressExtensionProperties(salesOrder.ShippingAddress, erpSalesOrder.ShippingAddress);

                if (isAdyenOrder || isPaypalOrder || isApplePayOrder || isGiveXOrder || isAmazonOrder)
                {
                    var connector = GetPaymentConnectorInformation(retailContext);
                    var paymentProperties = PaymentProperty.ConvertXMLToPropertyArray(connector.ConnectorProperties);
                    var paymentHash = PaymentProperty.ConvertToHashtable(paymentProperties);


                    if (connector != null)
                    {

                        #region Card Payment Blob Token Generating

                        for (int i = 0; i < salesOrder.TenderLines.Count; i++)
                        {

                            PaymentProperty.GetPropertyValue(paymentHash, $"{PaymentConfigurations.MerchantAccount}", $"{PaymentConfigurations.ServiceAccountId}", out string serviceAccountId);
                            PaymentProperty.GetPropertyValue(paymentHash, $"{GenericNamespace.Connector}", $"{ConnectorProperties.ConnectorName}", out string connecterName);

                            var tender = salesOrder.TenderLines[i];
                            var cardProperties = ConvertToPaymentProperties(cardPaymentProperties.Take((i + 1) * 17).Skip(i * 17).ToList());


                            cardProperties.Insert(0, new PaymentProperty($"{PaymentConfigurations.MerchantAccount}", $"{PaymentConfigurations.ServiceAccountId}", serviceAccountId));
                            cardProperties.Insert(1, new PaymentProperty($"{GenericNamespace.Connector}", $"{ConnectorProperties.ConnectorName}", connectorName));

                            tender.CardToken = PaymentProperty.ConvertPropertyArrayToXML(cardProperties.ToArray());

                            #endregion

                            #region Auth Payment Blob Token Generating

                            var authProperties = ConvertToPaymentProperties(authPaymentProperties.Take((i + 1) * 18).Skip(i * 18).ToList());

                            authProperties.Insert(0, new PaymentProperty($"{PaymentConfigurations.MerchantAccount}", $"{PaymentConfigurations.ServiceAccountId}", serviceAccountId));
                            authProperties.Insert(1, new PaymentProperty($"{GenericNamespace.Connector}", $"{ConnectorProperties.ConnectorName}", connectorName));

                            tender.Authorization = PaymentProperty.ConvertPropertyArrayToXML(authProperties.ToArray());

                            

                            #endregion
                        }
                    }
                    else
                    {
                        throw new Exception($"Getting error while fetching payment connector details from D365!");
                    }
                }

                AddHeaderAttributes(salesOrder, headerAttributes);
                AddLineAttributes(salesOrder, erpSalesOrder);
                AddExtensionProperties(salesOrder, headerExtensionProperties);

                var properties = CheckProductExistance(retailContext, salesOrder);

                salesOrder.ChannelId = retailContext.BaseChannelId;

                
                var order = manager.VSI_UploadOrder(salesOrder, properties).GetAwaiter().GetResult();


                if (isOnAccountOrder)
                {
                    Task.Delay(1000);

                    manager.VSI_UpdateOnAccountOrderPaymentTrans(order.Id, retailContext.BaseChannelId, 1, 1, salesOrder.CustomerId, onAccountTenderTypeId, retailContext.BaseCompany);
                }
                if (isiGlobalCreditCard)
                {
                    Task.Delay(1000);

                    manager.VSI_UpdateOnAccountOrderPaymentTrans(order.Id, retailContext.BaseChannelId, 1, 1, salesOrder.CustomerId, iGlobalTenderTypeId, retailContext.BaseCompany);
                }
                if (isCheckMoOrder)
                {
                    Task.Delay(1000);

                    manager.VSI_UpdateOnAccountOrderPaymentTrans(order.Id, retailContext.BaseChannelId, 1, 1, salesOrder.CustomerId, checkMoTenderTypeId, retailContext.BaseCompany);
                }
                if (isGiveXOrder)
                {
                    var givexTender = salesOrder.TenderLines.Where(t => t.TenderTypeId == "5");
                    foreach (var line in givexTender)
                    {
                        int lineNumber = (int)line.LineNumber;
                        string cardOrAccount = line.MaskedCardNumber.ToString();
                        Task.Delay(1000);

                        manager.VSI_UpdateOnAccountOrderPaymentTrans(order.Id, retailContext.BaseChannelId, lineNumber, 1, cardOrAccount, GiveXTenderTypeId, retailContext.BaseCompany);
                    }
                }
                SendOrderCreationResponseToEcomAsync(telemetryClient, responseApi, responseApiToken, functionName, erpSalesOrder.EcomSalesId, "Sales order created scussesfully!").GetAwaiter().GetResult();

                return order?.Id ?? string.Empty;
            }
            catch (Exception exception)
            {
                var ex = CommonUtility.GetDepthInnerException(exception);

                var message = ex.Message;

                if (ex is DataValidationException validationException)
                {
                    message = JsonConvert.SerializeObject(validationException.ValidationResults);
                }
                if (ex is PaymentException paymentException)
                {
                    message = JsonConvert.SerializeObject(paymentException.PaymentSdkErrors);
                }
                else if (ex is PaymentConfigurationException paymentConfigException)
                {
                    message = JsonConvert.SerializeObject(paymentConfigException.PaymentConfigurationSdkErrors);
                }

                SendOrderCreationResponseToEcomAsync(telemetryClient, responseApi, responseApiToken, functionName, erpSalesOrder.EcomSalesId, message).GetAwaiter().GetResult();

                throw ex;
            }
        }

        private static void AddExtensionProperties(SalesOrder salesOrder, List<KeyValue> properties)
        {
            var extensionProperties = new ObservableCollection<CommerceProperty>();
            foreach (var property in properties)
            {
                extensionProperties.Add(new CommerceProperty
                {
                    Key = property.Key,
                    Value = new CommercePropertyValue
                    {
                        StringValue = property.Value
                    }
                });
            }

            salesOrder.ExtensionProperties = extensionProperties;
        }

        private static void AddHeaderAttributes(SalesOrder salesOrder, List<KeyValue> headerAttributes)
        {
            var attributes = new ObservableCollection<AttributeValueBase>();
            
            foreach (var headerAttribute in headerAttributes)
            {
                
                attributes.Add(new AttributeTextValue
                {

                    Name = headerAttribute.Key,
                    TextValue = headerAttribute.Value
                });

            }
            //var distinctAttributes = attributes.Where(a => a.Key.Count() > 1).Select(a => a.Key.FirstOrDefault());
            salesOrder.AttributeValues = attributes;
        }

        private static List<PaymentProperty> ConvertToPaymentProperties(List<ErpPaymentProperties> erpPaymentProperties)
        {
            var paymentProperties = new List<PaymentProperty>();
            foreach (var prop in erpPaymentProperties)
            {          
                    switch (prop.ValueType)
                    {
                        case "String":
                            paymentProperties.Add(new PaymentProperty(prop.Namespace, prop.Name, prop.Value as string));
                            break;
                        case "Decimal":
                            paymentProperties.Add(new PaymentProperty(prop.Namespace, prop.Name, Convert.ToDecimal(string.IsNullOrEmpty(prop.Value as string)?null:prop.Value)));
                            break;
                        case "DateTime":
                            paymentProperties.Add(new PaymentProperty(prop.Namespace, prop.Name, Convert.ToDateTime(prop.Value)));
                            break;
                        case "PropertyList":
                            paymentProperties.Add(new PaymentProperty(prop.Namespace, prop.Name, ConvertToPaymentProperties(JsonConvert.DeserializeObject<List<ErpPaymentProperties>>(JsonConvert.SerializeObject(prop.Value))).ToArray()));
                            break;
                        default:
                            throw new Exception($"Payment value type is not supported!");
                    }               
            }

            return paymentProperties;
        }
        
        private static void AddLineAttributes(SalesOrder salesOrder, ErpSalesOrder erpSalesOrder)
        {
            foreach (var oLines in erpSalesOrder.SalesLines)
            {
                var attributes = new ObservableCollection<AttributeValueBase>();
                foreach (var paymentAttribute in oLines.LineAttributes)
                {
                    attributes.Add(new AttributeTextValue
                    {
                        Name = paymentAttribute.Key,
                        TextValue = paymentAttribute.Value
                    });
                }
                if (string.IsNullOrWhiteSpace(oLines.LinkedParentLineId))
                {
                    salesOrder.SalesLines.FirstOrDefault(x => x.ItemId == oLines.ItemId && x.LineNumber == oLines.LineNumber).AttributeValues = attributes;
                }
                else
                {
                    salesOrder.SalesLines.FirstOrDefault(x => x.ItemId == oLines.ItemId && x.LinkedParentLineId == oLines.LinkedParentLineId).AttributeValues = attributes;
                }
                
            }
        }

        private static SalesOrder InitNullPrimitiveCollection(SalesOrder salesOrder)
        {
            if (salesOrder.AffiliationLoyaltyTierLines == null) salesOrder.AffiliationLoyaltyTierLines = new ObservableCollection<SalesAffiliationLoyaltyTier>();
            if (salesOrder.AttributeValues == null) salesOrder.AttributeValues = new ObservableCollection<AttributeValueBase>();
            if (salesOrder.ChargeLines == null) salesOrder.ChargeLines = new ObservableCollection<ChargeLine>();
            if (salesOrder.DiscountCodes == null) salesOrder.DiscountCodes = new ObservableCollection<string>();
            if (salesOrder.IncomeExpenseLines == null) salesOrder.IncomeExpenseLines = new ObservableCollection<IncomeExpenseLine>();
            if (salesOrder.LoyaltyRewardPointLines == null) salesOrder.LoyaltyRewardPointLines = new ObservableCollection<LoyaltyRewardPointLine>();
            if (salesOrder.ReasonCodeLines == null) salesOrder.ReasonCodeLines = new ObservableCollection<ReasonCodeLine>();
            if (salesOrder.TenderLines == null) salesOrder.TenderLines = new ObservableCollection<TenderLine>();
            if (salesOrder.ContactInformationCollection == null) salesOrder.ContactInformationCollection = new ObservableCollection<ContactInformation>();
            if (salesOrder.TaxLines == null) salesOrder.TaxLines = new ObservableCollection<TaxLine>();
            if (salesOrder.ExtensionProperties == null) salesOrder.ExtensionProperties = new ObservableCollection<CommerceProperty>();
            if (salesOrder.ShippingAddress != null) if (salesOrder.ShippingAddress.ExtensionProperties == null) salesOrder.ShippingAddress.ExtensionProperties = new ObservableCollection<CommerceProperty>();
            if (salesOrder.SalesLines == null) salesOrder.SalesLines = new ObservableCollection<SalesLine>();

            if (salesOrder.ContactInformationCollection != null)
            {
                foreach (var c in salesOrder.ContactInformationCollection)
                {
                    if (c.ExtensionProperties == null)
                    {
                        c.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }
            }

            foreach (var c in salesOrder.ChargeLines)
            {
                if (c.TaxLines == null) c.TaxLines = new ObservableCollection<TaxLine>();
                if (c.ExtensionProperties == null) c.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                if (c.ReturnTaxLines == null) c.ReturnTaxLines = new ObservableCollection<TaxLine>();
                if (c.ChargeLineOverrides == null) c.ChargeLineOverrides = new ObservableCollection<ChargeLineOverride>();
                if (c.TaxMeasures == null) c.TaxMeasures = new ObservableCollection<TaxMeasure>();
                if (c.ReasonCodeLines == null) c.ReasonCodeLines = new ObservableCollection<ReasonCodeLine>();
            }

            foreach (var tenderLine in salesOrder.TenderLines)
            {
                if (tenderLine.ReasonCodeLines == null) tenderLine.ReasonCodeLines = new ObservableCollection<ReasonCodeLine>();
                if (tenderLine.ExtensionProperties == null) tenderLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
            }

            foreach (var salesLine in salesOrder.SalesLines)
            {
                if (salesLine.PeriodicDiscountPossibilities == null) salesLine.PeriodicDiscountPossibilities = new ObservableCollection<DiscountLine>();
                if (salesLine.ReasonCodeLines == null) salesLine.ReasonCodeLines = new ObservableCollection<ReasonCodeLine>();
                if (salesLine.ChargeLines == null) salesLine.ChargeLines = new ObservableCollection<ChargeLine>();
                if (salesLine.DiscountLines == null) salesLine.DiscountLines = new ObservableCollection<DiscountLine>();
                if (salesLine.PriceLines == null) salesLine.PriceLines = new ObservableCollection<PriceLine>();
                if (salesLine.TaxLines == null) salesLine.TaxLines = new ObservableCollection<TaxLine>();
                if (salesLine.RelatedDiscountedLineIds == null) salesLine.RelatedDiscountedLineIds = new ObservableCollection<string>();
                if (salesLine.ReturnTaxLines == null) salesLine.ReturnTaxLines = new ObservableCollection<TaxLine>();
                if (salesLine.ExtensionProperties == null) salesLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();

                foreach (var priceLine in salesLine.PriceLines)
                {
                    if (priceLine.ExtensionProperties == null) priceLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                }

                foreach (var discountLine in salesLine.DiscountLines)
                {
                    if (discountLine.ExtensionProperties == null)
                    {
                        discountLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }

                foreach (var discountLine in salesLine.TaxLines)
                {
                    if (discountLine.ExtensionProperties == null)
                    {
                        discountLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }

                foreach (var chargeLine in salesLine.ChargeLines)
                {
                    if (chargeLine.TaxLines == null) chargeLine.TaxLines = new ObservableCollection<TaxLine>();
                    if (chargeLine.ExtensionProperties == null) chargeLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    if (chargeLine.ReturnTaxLines == null) chargeLine.ReturnTaxLines = new ObservableCollection<TaxLine>();
                    if (chargeLine.ChargeLineOverrides == null) chargeLine.ChargeLineOverrides = new ObservableCollection<ChargeLineOverride>();
                    if (chargeLine.TaxMeasures == null) chargeLine.TaxMeasures = new ObservableCollection<TaxMeasure>();
                    if (chargeLine.ReasonCodeLines == null) chargeLine.ReasonCodeLines = new ObservableCollection<ReasonCodeLine>();
                    if (chargeLine.ExtensionProperties == null)
                    {
                        chargeLine.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }

                if (salesLine.ShippingAddress != null) if (salesLine.ShippingAddress.ExtensionProperties == null) salesLine.ShippingAddress.ExtensionProperties = new ObservableCollection<CommerceProperty>();
            }

            return salesOrder;
        }

        private static List<CommerceProperty> CheckProductExistance(D365RetailServerContext retailContext, SalesOrder salesOrder)
        {
            try
            {
                var manager = retailContext.FactoryManager.GetManager<IProductManager>();
                var productLookupClauses = salesOrder.SalesLines.Select(s => new ProductLookupClause { ItemId = s.ItemId.Split('-')[0] });
                productLookupClauses = productLookupClauses.GroupBy(x => x.ItemId).Select(x => x.First()).ToList();
                var products = manager.Search(new ProductSearchCriteria
                {
                    Context = new ProjectionDomain { ChannelId = retailContext.BaseChannelId },
                    ItemIds = new ObservableCollection<ProductLookupClause>(productLookupClauses),
                    DataLevelValue = (int)CommerceEntityDataLevel.Minimal
                },
                new QueryResultSettings
                {
                    Paging = new PagingInfo
                    {
                        Skip = 0,
                        Top = 1000
                    }
                }).GetAwaiter().GetResult();

                if (products.Count() != productLookupClauses.Count())
                {
                    var productNumbers = products.Select(s => s.ProductNumber.ToString()).ToList();

                    throw new Exception($"Product Details not found of varients ({string.Join(",", salesOrder.SalesLines.Where(s => !productNumbers.Contains(s.ItemId)).Select(p => p.ItemId))})!");
                }

                var salesLines = salesOrder.SalesLines;
                var commerceProperty = new List<CommerceProperty>();

                foreach (var salesLine in salesLines)
                {
                    var product = products.Where(p => p.ProductNumber == salesLine.ItemId.Split('-')[0])?.FirstOrDefault();

                    if (salesLine.ItemId.Contains('-'))
                    {
                        var variants = product.CompositionInformation?.VariantInformation?.Variants;
                        if (variants == null || variants.Count == 0)
                        {
                            throw new Exception($"Product Details not found of varients ({salesLine.ItemId})!");
                        }

                        var variant = variants.Where(v => v.ProductNumber == salesLine.ItemId)?.FirstOrDefault();

                        if (variant != null)
                        {
                            salesLine.ProductId = variant.DistinctProductVariantId;
                            salesLine.ItemId = salesLine.ItemId.Split('-')[0];
                            salesLine.MasterProductId = variant.MasterProductId;
                            salesLine.UnitOfMeasureSymbol = product.Rules?.DefaultUnitOfMeasure;
                            salesLine.SalesOrderUnitOfMeasure = product.Rules?.DefaultUnitOfMeasure;

                            commerceProperty.Add(new CommerceProperty
                            {
                                Key = salesLine.ProductId.ToString(),
                                Value = new CommercePropertyValue
                                {
                                    StringValue = variant.VariantId
                                }
                            });
                        }
                        else
                        {
                            throw new Exception($"Product Details not found of varients ({salesLine.ItemId})!");
                        }
                    }
                    else
                    {
                        var variants = product.CompositionInformation?.VariantInformation?.Variants;

                        if (variants != null && variants.Count > 0)
                        {
                            var variant = variants.Where(v => v.ItemId == salesLine.ItemId && v.ConfigId == "Default")?.FirstOrDefault();
                            if (variant != null)
                            {
                                salesLine.ProductId = variant.DistinctProductVariantId;
                                salesLine.ItemId = variant.ItemId;
                                salesLine.MasterProductId = variant.MasterProductId;
                                salesLine.UnitOfMeasureSymbol = product.Rules?.DefaultUnitOfMeasure;
                                salesLine.SalesOrderUnitOfMeasure = product.Rules?.DefaultUnitOfMeasure;

                                commerceProperty.Add(new CommerceProperty
                                {
                                    Key = salesLine.ProductId.ToString(),
                                    Value = new CommercePropertyValue
                                    {
                                        StringValue = variant.VariantId
                                    }
                                });

                            }
                        }
                        else
                        {
                            salesLine.ProductId = product.RecordId;
                            salesLine.UnitOfMeasureSymbol = product.Rules?.DefaultUnitOfMeasure;
                            salesLine.SalesOrderUnitOfMeasure = product.Rules?.DefaultUnitOfMeasure;
                        }


                    }
                }

                return commerceProperty;
            }
            catch (Exception exception)
            {
                throw CommonUtility.GetDepthInnerException(exception);
            }
        }

        private static VSI_RT.PaymentConnectorConfiguration GetPaymentConnectorInformation(D365RetailServerContext retailContext)
        {
            var manager = retailContext.FactoryManager.GetManager<VSI_RT.IVSICardPaymentManager>();

            var paymentConnectors = manager.VSI_GetMerchantInformation(retailContext.BaseChannelId).GetAwaiter().GetResult();

            return paymentConnectors?.PaymentConnectorConfiguration?.FirstOrDefault();
        }

        private static void AddShippingAddressExtensionProperties(Address address, ErpAddress erpAddresses)
        {
            var properties = new ObservableCollection<CommerceProperty>();
            foreach (var property in erpAddresses.AddressExtensionProperties)
            {
                properties.Add(new CommerceProperty
                {
                    Key = property.Key,
                    Value = new CommercePropertyValue
                    {
                        StringValue = property.Value
                    }
                });
            }

            address.ExtensionProperties = properties;
        }

        private static async Task SendOrderCreationResponseToEcomAsync(TelemetryClient telemetryClient, string api, string token, string functionName, string orderId, string message)
        {
            dynamic magentoRequest = new
            {
                response = new
                {
                    order_id = orderId,
                    message = message
                }
            };

            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", $"Bearer {token}");

                var response = await client.PostAsync(api, new StringContent(JsonConvert.SerializeObject(magentoRequest), Encoding.UTF8, "application/json"));

                var responseString = await response.Content.ReadAsStringAsync();

                telemetryClient.TrackTrace(functionName, $"Magento service response: {responseString}");

                if (response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    telemetryClient.TrackException(new Exception(responseString));
                }
            }
        }

        #endregion
    }
}