using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HealthChecks.CosmosDb
{
    public class CosmosDbHealthCheck
        : IHealthCheck
    {
        private static readonly ConcurrentDictionary<string, CosmosClient> _connections = new ConcurrentDictionary<string, CosmosClient>();

        private readonly string _connectionString;
        private readonly string _database;
        private readonly IEnumerable<string> _containers;
        private readonly CosmosClientOptions _cosmosClientOptions;

        public CosmosDbHealthCheck(string connectionString, CosmosClientOptions cosmosClientOptions = default)
            : this(connectionString, default, default, cosmosClientOptions) {
              _cosmosClientOptions = cosmosClientOptions;
            }

        public CosmosDbHealthCheck(string connectionString, string database, CosmosClientOptions cosmosClientOptions = default)
            : this(connectionString, database, default, cosmosClientOptions)
        {
            _database = database;
            _cosmosClientOptions = cosmosClientOptions;
        }
        public CosmosDbHealthCheck(string connectionString, string database, IEnumerable<string> containers, CosmosClientOptions cosmosClientOptions = default)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _database = database;
            _containers = containers;
            _cosmosClientOptions = cosmosClientOptions;
        }
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!_connections.TryGetValue(_connectionString, out var cosmosDbClient))
                {
                    cosmosDbClient = new CosmosClient(_connectionString, _cosmosClientOptions);

                    if (!_connections.TryAdd(_connectionString, cosmosDbClient))
                    {
                        cosmosDbClient.Dispose();
                        cosmosDbClient = _connections[_connectionString];
                    }
                }

                await cosmosDbClient.ReadAccountAsync();

                if (_database != null)
                {
                    var database = cosmosDbClient.GetDatabase(_database);
                    await database.ReadAsync();

                    if (_containers != null && _containers.Any())
                    {
                        foreach (var container in _containers)
                        {
                            await database.GetContainer(container)
                                .ReadContainerAsync();
                        }
                    }
                }

                return HealthCheckResult.Healthy();
            }
            catch (Exception ex)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, exception: ex);
            }
        }
    }
}
