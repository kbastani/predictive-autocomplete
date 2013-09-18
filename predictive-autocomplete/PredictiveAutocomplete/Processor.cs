using Microsoft.WindowsAzure.Storage;
using Neo4jClient;
using PredictiveAutocomplete.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace PredictiveAutocomplete
{
    

    public interface IGraphNode
    {
        string label { get; set; }
        int size { get; set; }
    }

    public class Processor
    {

        /// <summary>
        /// Index an autocomplete query to blob storage container that is accessible over public HTTP.
        /// </summary>
        /// <param name="queryKey">A list of queries to be added to the index. Queries are ranked on A.size.</param>
        /// <param name="storage">The Windows Azure cloud storage account that will be used for blob storage.</param>
        /// <param name="cacheId">The container name that will be used. This container must have public accessibility.</param>
        /// <param name="resultsPerFile">
        /// The number of results maximum that should be indexed per file. 
        /// If you only want 5 results maximum to display in your search box then set this value to 5.</param>
        /// <param name="maxLength">
        /// The max length property is the number of symbols to index per supplied query key.
        /// If you do not expect queries to be ambiguous beyond a certain length, it is advisible to set this value to correspond with the volume of query keys in your index. 
        /// For example if 40 character queries only have 1 result over the entire index then set this property to 40. 
        /// </param>
        public static void IndexAutoCompleteKey(List<IGraphNode> queryKey, CloudStorageAccount storage, string cacheId, int resultsPerFile, int maxLength)
        {
            try
            {
                queryKey.AsParallel().ForAll(phrs => AutoCompleteProcessor(phrs, storage, cacheId, maxLength));
            }
            catch (Exception ex)
            {
                throw ex;
            }
            
        }

        private static void AutoCompleteProcessor(IGraphNode queryKey, CloudStorageAccount storage, string cacheId, int maxLength)
        {

            string containerId = cacheId;

            // Use paralellization for character combinations up to 5 characters
            List<string> keyList = new List<string>();

            // Key list
            for (int i = 0; i < Math.Min(queryKey.label.Length, maxLength); i++)
            {
                keyList.Add(queryKey.label.Substring(0, i + 1).ToUpperInvariant());
            }

            keyList.AsParallel().ForAll(key =>
            {
                // Retrieve files from cloud storage
                Stream blobStream = BlobService.GetBlob(storage, containerId, "cache/" + key.ToLowerInvariant());

                if (blobStream == null)
                {
                    blobStream = CreateAutocompleteBlob(queryKey, storage, containerId, key);
                }
                else
                {
                    // Deserialize the stream from blob storage
                    JavaScriptSerializer jsSerializer = new JavaScriptSerializer();
                    try
                    {
                        var acBlock = jsSerializer.Deserialize<GraphNode[]>(Regex.Replace(new StreamReader(blobStream).ReadToEnd(), @"^dataCallback\(|\)$", "", RegexOptions.IgnoreCase));
                        var acKey = new GraphNode() { label = queryKey.label.ToLowerInvariant(), size = queryKey.size };

                        // Check for key phrase
                        var hasKey = acBlock.ToList().Any(aKey => aKey.label.Equals(acKey.label, StringComparison.InvariantCultureIgnoreCase));

                        // Update key weight and pushed back to storage
                        if (hasKey)
                        {
                            var keyIndex = acBlock.ToList().IndexOf(acBlock.Where(aKey => aKey.label.Equals(acKey.label, StringComparison.InvariantCultureIgnoreCase)).First());
                            acBlock[keyIndex] = acKey;
                            var acBlockList = acBlock.ToList();
                            acBlockList = acBlockList.OrderByDescending(acbk => acbk.size).ToList().Take(10).ToList();
                            acBlock = acBlockList.ToArray();
                        }
                        else
                        {
                            var acBlockList = acBlock.ToList();
                            acBlockList.Add(acKey);
                            acBlockList = acBlockList.OrderByDescending(acbk => acbk.size).ToList().Take(10).ToList();
                            acBlock = acBlockList.ToArray();
                        }

                        // Update blob
                        var jsonString = string.Format("dataCallback({0})", jsSerializer.Serialize(acBlock));
                        blobStream = new MemoryStream();
                        var bytes = Encoding.UTF8.GetBytes(jsonString);
                        blobStream.Write(bytes, 0, bytes.Length);
                        blobStream.Seek(0, SeekOrigin.Begin);
                        BlobService.PutBlob(storage, containerId, key.ToLowerInvariant(), blobStream, "cache", 0);
                    }
                    catch (Exception)
                    {
                        blobStream = CreateAutocompleteBlob(queryKey, storage, containerId, key);
                    }
                }
            });
        }

        private static Stream CreateAutocompleteBlob(IGraphNode queryKey, CloudStorageAccount storage, string containerId, string key)
        {
            Stream blobStream;
            // Create new serialized autocomplete model containing phrase and weight data
            JavaScriptSerializer jsSerializer = new JavaScriptSerializer();
            var acBlock = new List<string>() { key }.Select(kv => new GraphNode() { label = queryKey.label.ToLowerInvariant(), size = queryKey.size }).ToArray();
            var jsonString = string.Format("dataCallback({0})", jsSerializer.Serialize(acBlock));
            blobStream = new MemoryStream();
            var bytes = Encoding.UTF8.GetBytes(jsonString);
            blobStream.Write(bytes, 0, bytes.Length);
            blobStream.Seek(0, SeekOrigin.Begin);
            BlobService.PutBlob(storage, containerId, key.ToLowerInvariant(), blobStream, "cache", 0);
            return blobStream;
        }

        public static Neo4jClient.HttpClientWrapper GetNeo4jAuthenticatedClient()
        {
            // Get authentication header from configuration
            var authentication = Configuration.GetAuthorizationHeader();

            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Authorization", Convert.ToBase64String(Encoding.ASCII.GetBytes(authentication)));
            Neo4jClient.HttpClientWrapper clientWrapper = new Neo4jClient.HttpClientWrapper(httpClient);
            return clientWrapper;
        }

        public static GraphClient GetReadNeo4jGraphClient()
        {
            Neo4jClient.HttpClientWrapper clientWrapper = Processor.GetNeo4jAuthenticatedClient();
            Neo4jClient.GraphClient graphClient = new Neo4jClient.GraphClient(new Uri(Configuration.GetReadDatabaseUri()), clientWrapper);
            return graphClient;
        }

        public static GraphClient GetNeo4jGraphClient()
        {
            Neo4jClient.HttpClientWrapper clientWrapper = Processor.GetNeo4jAuthenticatedClient();
            Neo4jClient.GraphClient graphClient = new Neo4jClient.GraphClient(new Uri(Configuration.GetDatabaseUri()), clientWrapper);
            return graphClient;
        }


        /// <summary>
        /// Get ranked nodes from Neo4j graph database using a templated cypher query that queries an index using a supplied valid lucene query. 
        /// Each node in the index has a weight rating by specifying a relationship name which is used to determine the 
        /// distinct number of incoming links to each node in your query with that relationship name. 
        /// You must specify the valid property name that is to be used as the label for the autocomplete search. 
        /// For example, if you are querying a database of books and wanted to list the names of books in the 
        /// autocomplete search, then the label property would be "Title" where each book node b has b.Title as the book name.
        /// </summary>
        /// <param name="index">The case sensitive Neo4j node index name that you want to query.</param>
        /// <param name="luceneQuery">The valid lucene query that you want to use to query the supplied index.</param>
        /// <param name="relationshipLabel">
        /// The relationship name that will be used to determine the number of incoming links to each node you are querying for. 
        /// Leave blank if you want to query all incoming links regardless of relationship type.</param>
        /// <param name="labelPropertyName">The label property name for each of your Neo4j nodes. See method summary for details.</param>
        /// <param name="skip">Skip a number of a nodes, ordered by the Neo4j assigned node id of each node you are querying for. Use this for processing batches on large graph queries.</param>
        /// <param name="limit">Limit the number of results you would like returned. Use this in combination with the skip property to process batches on large graph queries.</param>
        /// <returns>Returns a list of nodes that implements IGraphNode interface, having a label and size property for ranking result order.</returns>
        public static List<IGraphNode> GetRankedNodesForQuery(string index, string luceneQuery, string relationshipLabel, string labelPropertyName, int skip, int limit)
        {
            var sb = new StringBuilder();
            sb.AppendLine("START node=node:{0}(\"{1}\")");
            sb.AppendLine("WITH node");
            sb.AppendLine("SKIP {2}");
            sb.AppendLine("LIMIT {3}");
            sb.AppendLine("WITH node");
            sb.AppendLine("MATCH n-[{4}]->node");
            sb.AppendLine("WITH node, count(distinct n) as size");
            sb.AppendLine("RETURN node.{5}? as label, size");
            sb.AppendLine("ORDER BY id(node)");
            sb.AppendLine("LIMIT {3}");

            string commandQuery = sb.ToString();

            commandQuery = string.Format(commandQuery, index, luceneQuery, skip, limit, !string.IsNullOrEmpty(relationshipLabel) ? string.Format(":{0}", relationshipLabel) : string.Empty, labelPropertyName);

            GraphClient graphClient = GetNeo4jGraphClient();

            var cypher = new CypherFluentQueryCreator(graphClient, new CypherQueryCreator(commandQuery), new Uri(Configuration.GetDatabaseUri()));

            var resulttask = cypher.ExecuteGetCypherResults<GraphNode>();
            var graphNodeResults = resulttask.Result.ToList().Select(gn => (IGraphNode)gn).ToList();
            return graphNodeResults;
        }
    }


    /// <summary>
    /// This class represents a graph node that is ranked on size and has a unique label.
    /// </summary>
    public class GraphNode : IGraphNode
    {
        string _label;
        int _size;

        public string label
        {
            get
            {
                return _label;
            }
            set
            {
                _label = value;
            }
        }

        public int size
        {
            get
            {
                return _size;
            }
            set
            {
                _size = value;
            }
        }
    }
}
