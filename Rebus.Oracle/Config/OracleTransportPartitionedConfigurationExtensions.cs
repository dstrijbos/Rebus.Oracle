using System;
using Rebus.Logging;
using Rebus.Oracle;
using Rebus.Oracle.Transport;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config
{    /// <summary>
     /// Configuration extensions for the partitioned SQL transport
     /// </summary>
    public static class OracleTransportPartitionedConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use Oracle as its transport, with partitioning. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseOraclePartitioned(this StandardConfigurer<ITransport> configurer, string connectionString, string tableName, string inputQueueName, bool enlistInAmbientTransaction = false, bool enablePersistence = false, bool automaticallyCreateTables = true)
        {
            Configure(configurer, loggerFactory => new OracleFactory(connectionString, null, enlistInAmbientTransaction), tableName, inputQueueName, enablePersistence, automaticallyCreateTables);
        }

        static void Configure(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, OracleFactory> connectionProviderFactory, string tableName, string inputQueueName, bool enablePersistence = false, bool automaticallyCreateTables = true)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
                var rebusTime = context.Get<IRebusTime>();
                var connectionProvider = connectionProviderFactory(rebusLoggerFactory);
                var transport = new OraclePartitionedTransport(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory, rebusTime, enablePersistence);

                if (automaticallyCreateTables)
                    transport.EnsureTableIsCreated();

                return transport;
            });

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });
        }
    }
}
