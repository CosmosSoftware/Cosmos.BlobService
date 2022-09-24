using Cosmos.BlobService.Config;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Cosmos.BlobService
{
    /// <summary>
    /// Adds the Cosmos Storage Context to the Services Collection
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the storage context to the services collection.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="config"></param>
        public static void AddCosmosStorageContext(this IServiceCollection services, IConfiguration config)
        {
            // Azure Parameters
            var azureBlobStorageConnectionString = GetKeyValue(config, "AzureBlobStorageConnectionString");
            var azureBlobStorageContainerName = GetKeyValue(config, "AzureBlobStorageContainerName");
            var azureBlobStorageEndPoint = GetKeyValue(config, "AzureBlobStorageEndPoint");

            // Amazon Parameters
            var amazonAwsAccessKeyId = GetKeyValue(config, "AmazonAwsAccessKeyId");
            var amazonAwsSecretAccessKey = GetKeyValue(config, "AmazonAwsSecretAccessKey");
            var amazonBucketName = GetKeyValue(config, "AmazonBucketName");
            var amazonRegion = GetKeyValue(config, "AmazonRegion");
            var profileName = "aws";
            var serviceUrl = GetKeyValue(config, "AmazonServiceUrl");


            var cosmosConfig = new CosmosStorageConfig();
            cosmosConfig.PrimaryCloud = "azure";
            cosmosConfig.StorageConfig = new StorageConfig();


            cosmosConfig.StorageConfig.AzureConfigs.Add(new AzureStorageConfig()
            {
                AzureBlobStorageConnectionString = azureBlobStorageConnectionString,
                AzureBlobStorageContainerName = azureBlobStorageContainerName,
                AzureBlobStorageEndPoint = azureBlobStorageEndPoint
            });

            cosmosConfig.StorageConfig.AmazonConfigs.Add(new AmazonStorageConfig()
            {
                AmazonAwsAccessKeyId = amazonAwsAccessKeyId,
                AmazonAwsSecretAccessKey = amazonAwsSecretAccessKey,
                AmazonBucketName = amazonBucketName,
                AmazonRegion = amazonRegion,
                ProfileName = profileName,
                ServiceUrl = serviceUrl
            });

            services.AddSingleton(cosmosConfig);
            services.AddTransient<StorageContext>();

        }

        private static string GetKeyValue(IConfiguration config, string key)
        {
            var data = config[key];
            if (string.IsNullOrEmpty(data))
            {
                data = Environment.GetEnvironmentVariable(key);
                if (string.IsNullOrEmpty(data))
                {
                    data = Environment.GetEnvironmentVariable(key.ToUpper());
                }
            }
            return data;
        }
    }

}
