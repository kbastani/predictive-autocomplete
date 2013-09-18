using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace PredictiveAutocomplete.Services
{
    public class BlobService
    {
        public static Stream GetBlob(CloudStorageAccount storage, string blobFolder, string query)
        {
            CloudBlobClient blobClient = storage.CreateCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.ListContainers(blobFolder, ContainerListingDetails.All).FirstOrDefault();
            if (blobContainer == null)
            {
                return null;
            }

            CloudBlockBlob blob = blobContainer.GetBlockBlobReference(query);

            if (Exists(blob))
            {
                Encoding encoding = Encoding.UTF8;
                MemoryStream memoryStream = new MemoryStream();
                blob.DownloadToStream(memoryStream);
                memoryStream.Seek(0, SeekOrigin.Begin);
                return memoryStream;
            }
            else
            {
                return null;
            }
        }

        public static bool PutBlob(CloudStorageAccount storage, string blobFolder, string blobName, Stream blobStream, string query, int tryCount)
        {

            try
            {
                CloudBlobClient blobClient = storage.CreateCloudBlobClient();
                CloudBlobContainer blobContainer = blobClient.ListContainers(blobFolder, ContainerListingDetails.All).FirstOrDefault();
                if (blobContainer == null)
                {
                    blobContainer = blobClient.GetContainerReference(blobFolder);
                    blobContainer.Create();
                }
                CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference(HttpUtility.UrlDecode(query));
                CloudBlockBlob cloudBlob = blobDirectory.GetBlockBlobReference(blobName);

                string newLeaseId = Guid.NewGuid().ToString();

                var accessCondition = AccessCondition.GenerateLeaseCondition(newLeaseId);

                bool cloudBlobExists = false;

                try
                {
                    bool exists = cloudBlob.Exists();
                    cloudBlobExists = exists;
                }
                catch
                {
                    cloudBlobExists = false;
                }

                if (cloudBlobExists ? !string.IsNullOrEmpty(cloudBlob.AcquireLease(TimeSpan.FromSeconds(30), accessCondition.LeaseId, accessCondition)) : true)
                {
                    byte[] content = new byte[blobStream.Length];
                    blobStream.Read(content, 0, (int)blobStream.Length);
                    var blockLength = 400 * 1024;
                    var numberOfBlocks = ((int)content.Length / blockLength) + 1;
                    string[] blockIds = new string[numberOfBlocks];

                    try
                    {
                        Parallel.For(0, numberOfBlocks, x =>
                        {
                            var blockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                            var currentLength = Math.Min(blockLength, content.Length - (x * blockLength));

                            using (var memStream = new MemoryStream(content, x * blockLength, currentLength))
                            {
                                if (cloudBlobExists)
                                {
                                    try
                                    {
                                        cloudBlob.PutBlock(blockId, memStream, "", accessCondition);
                                    }
                                    catch (Exception)
                                    {
                                        cloudBlob.PutBlock(blockId, memStream, "");
                                    }
                                }
                                else
                                {
                                    cloudBlob.PutBlock(blockId, memStream, "");
                                }

                            }
                            blockIds[x] = blockId;
                        });
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(string.Format("Parallel put block error occurred: {0}", ex.Message), ex);
                    }


                    if (cloudBlobExists)
                    {
                        cloudBlob.AcquireLease(TimeSpan.FromSeconds(30), accessCondition.LeaseId, accessCondition);

                        cloudBlob.PutBlockList(blockIds, accessCondition, new BlobRequestOptions() { RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.LinearRetry() });

                        // Set properties
                        cloudBlob.Properties.ContentType = "application/json";
                        cloudBlob.SetProperties(accessCondition);

                        // Quickly clear this data from memory
                        blobStream.Dispose();

                        cloudBlob.ReleaseLease(accessCondition);
                    }
                    else
                    {
                        cloudBlob.PutBlockList(blockIds, null, new BlobRequestOptions() { RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.LinearRetry() });

                        // Set properties
                        cloudBlob.Properties.ContentType = "application/json";
                        cloudBlob.SetProperties();

                        // Quickly clear this data from memory
                        blobStream.Dispose();
                    }
                }
            }
            catch
            {
                if (tryCount < 10)
                {
                    PutBlob(storage, blobFolder, blobName, blobStream, query, tryCount + 1);
                }
            }

            return (!(tryCount >= 10));
        }


        public static bool Exists(CloudBlockBlob blob)
        {
            try
            {
                blob.FetchAttributes();
                return true;
            }
            catch (StorageException)
            {
                return false;
            }
        }
    }
}
