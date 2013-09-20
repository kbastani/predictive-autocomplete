using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PredictiveAutocomplete
{
    /// <summary>
    /// Utilities class used to load and retrieve platform configurations.
    /// </summary>
    public sealed class Configuration
    {
        #region Configuration Static Methods

        #region Private Fields

        // Configuration private fields
        private CloudStorageAccount _dataConnectionString = default(CloudStorageAccount);
        private string _neo4jConnectionString = string.Empty;
        private string _neo4jConnectionStringAuthentication = string.Empty;
        private string _autoCompleteCacheId = string.Empty;
        private string _blobStorageAddress = string.Empty;

        // Singleton state flag
        private bool _initialized = false;

        // Instance
        private static readonly Configuration _instance = new Configuration();

        #endregion

        #region Public Fields

        /// <summary>
        /// The data connection string to Microsoft Windows Azure Blob Storage.
        /// </summary>
        public static CloudStorageAccount DataConnectionString { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._dataConnectionString; } }

        /// <summary>
        /// The Neo4j connection string to this instance's memory store.
        /// </summary>
        public static string Neo4jConnectionString { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._neo4jConnectionString; } }

        /// <summary>
        /// The Neo4j connection string's authentication mechanism.
        /// </summary>
        public static string Neo4jConnectionStringAuthentication { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._neo4jConnectionStringAuthentication; } }

        /// <summary>
        /// The Auto Complete identifier for predictive search index.
        /// </summary>
        public static string AutoCompleteCacheId { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._autoCompleteCacheId; } }

        /// <summary>
        /// The blob storage URI for this instance.
        /// </summary>
        public static string BlobStorageAddress { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._blobStorageAddress; } }

        /// <summary>
        /// The instance of the singleton configuration class for the Core assembly.
        /// </summary>
        public static Configuration Instance { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance; } }

        /// <summary>
        /// Flag containing the initialization status of the singleton configuration class. If this flag is set to false,
        /// access to its other properties will result in an UnauthorizedAccessException.
        /// </summary>
        public static bool Initialized { get { _instance.Internal_CheckEnforceIllegalAccess(); return _instance._initialized; } }

        #endregion

        /// <summary>
        /// Private constructor for the singleton configuration class.
        /// </summary>
        private Configuration()
        {

        }

        /// <summary>
        /// Initialization method used to instantiate configuration data embedded as a resource in the Core assembly.
        /// </summary>
        public static void Initialize()
        {
            lock (_instance)
            {
                if (!_instance._initialized)
                {
                    // Initialize configuration values
                    _instance._autoCompleteCacheId = GetAutoCompleteAddress();
                    _instance._dataConnectionString = GetDataConnectionString();
                    _instance._neo4jConnectionString = GetNeo4jConnectionString();
                    _instance._neo4jConnectionStringAuthentication = GetNeo4jConnectionStringAuthentication();

                    // Set the initialized flag to true
                    _instance._initialized = true;
                }
            }
        }


        /// <summary>
        /// Check to enforce that the singleton configuration class has been properly initialized before access.
        /// </summary>
        /// <returns>Returns true of the singleton configuration class has been initialized.</returns>
        public static bool CheckEnforceIllegalAccess()
        {
            return _instance.Internal_CheckEnforceIllegalAccess();
        }

        /// <summary>
        /// Internal reference check to enforce that the singleton configuration class has been properly initialized before access.
        /// </summary>
        /// <returns>Returns true of the singleton configuration class has been initialized.</returns>
        private bool Internal_CheckEnforceIllegalAccess()
        {
            if (_instance._initialized)
            {
                return true;
            }
            else
            {
                try
                {
                    Configuration.Initialize();
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }

        #endregion

        public static CloudStorageAccount GetDataConnectionString()
        {
            try
            {
                if (CloudConfigurationManager.GetSetting("DataConnectionString") != null)
                {
                    string storageId = CloudConfigurationManager.GetSetting("DataConnectionString");
                    return CloudStorageAccount.Parse(storageId);
                }
            }
            catch (Exception)
            {
            }

            string connString = ConfigurationManager.AppSettings["DataConnectionString"];
            return CloudStorageAccount.Parse(connString);

        }


        public static string GetAutoCompleteCacheId()
        {
            if (Initialized)
            {
                return AutoCompleteCacheId;
            }
            else
            {
                return GetAutoCompleteAddress();
            }
        }

        public static string GetAutoCompleteAddress()
        {
            try
            {
                if (CloudConfigurationManager.GetSetting("AutoCompleteCacheId") != null)
                {
                    string cacheId = CloudConfigurationManager.GetSetting("AutoCompleteCacheId");
                    return cacheId;
                }
            }
            catch (Exception)
            {
            }


            return ConfigurationManager.AppSettings["AutoCompleteCacheId"];

        }

        public static string GetDatabaseUri()
        {
            if (Initialized)
            {
                return Neo4jConnectionString;
            }
            else
            {
                return GetNeo4jConnectionString();
            }
        }

        public static string GetNeo4jConnectionString()
        {
            try
            {
                if (CloudConfigurationManager.GetSetting("GuySwarm.MyAddress") != null)
                {
                    var databaseUri = CloudConfigurationManager.GetSetting("Neo4j.ConnectionString");
                    return databaseUri;
                }
            }
            catch (Exception)
            {
            }
            // Get database URI from configuration
            return ConfigurationManager.AppSettings["Neo4j.ConnectionString"];
        }

        public static string GetAuthorizationHeader()
        {
            if (Initialized)
            {
                return Neo4jConnectionStringAuthentication;
            }
            else
            {
                return GetNeo4jConnectionStringAuthentication();
            }
        }

        public static string GetNeo4jConnectionStringAuthentication()
        {
            try
            {
                if (CloudConfigurationManager.GetSetting("Neo4j.ConnectionString.Authentication") != null)
                {
                    var authentication = CloudConfigurationManager.GetSetting("Neo4j.ConnectionString.Authentication");
                    return authentication;
                }
            }
            catch (Exception)
            {
            }
            // Get database URI from configuration
            return ConfigurationManager.AppSettings["Neo4j.ConnectionString.Authentication"];
        }

    }

}
