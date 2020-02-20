using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Oracle.Schema;
using Rebus.Oracle.Transport;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.Oracle.Transport
{
    /// <summary>
    /// Variant of <see cref="OracleTransport"/> that uses Oracle to move grouped messages around
    /// </summary>
    public class OraclePartitionedTransport : ITransport, IInitializable, IDisposable
    {
        /// <summary>Header key of message GroupId, use this to partition your queue</summary>
        public const string GroupIdHeaderKey = "rbs2-msg-groupid";
        /// <summary>Header key of message Status, used for partitioned queues</summary>
        public const string StatusHeaderKey = "rbs2-msg-status";
        public const string ProcessingStatus = "Processing";
        public const string CompletedStatus = "Completed";

        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        readonly OracleFactory _factory;
        readonly DbName _table;
        readonly AsyncBottleneck _receiveBottleneck = new AsyncBottleneck(20);
        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly ILog _log;
        readonly IRebusTime _rebusTime;

        // SQL are cached so that strings are not built up at every command
        readonly string _sendSql, _receiveSql, _expiredSql;

        /// <summary>Gets the address of the transport</summary>
        public string Address { get; }

        /// <summary> </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName"></param>
        /// <param name="inputQueueName"></param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="rebusTime"></param>
        public OraclePartitionedTransport(OracleFactory connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            _log = rebusLoggerFactory.GetLogger<OraclePartitionedTransport>();
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _factory = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _table = new DbName(tableName);
            _sendSql = SendCommandPartitioned.Sql(_table);

            // One-way clients don't have an input queue to receive from or cleanup
            if (inputQueueName != null)
            {
                Address = inputQueueName;
                _receiveSql = BuildReceiveSql();
                _expiredSql = BuildExpiredSql();
                _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
            }
        }

        /// <inheritdoc />
        public void Initialize()
        {
            _expiredMessagesCleanupTask?.Start();
        }

        /// <summary>The Oracle transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        { }

        /// <inheritdoc />
        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var headers = message.Headers;
            var now = _rebusTime.Now;
            var priority = headers.GetMessagePriority();
            var visible = headers.GetInitialVisibilityDelay(now);
            var ttl = headers.GetTtlSeconds();
            var groupId = headers.GetGroupId();
            var serializedHeaders = HeaderSerializer.Serialize(headers);

            var command = context.GetSendCommandPartitioned(_factory, _sendSql);
            // Lock is blocking, but we're not async anyway (Oracle provider is blocking).
            // As a bonus: 
            // (1) Monitor is faster than SemaphoreSlim when there's no contention, which is usually the case; 
            // (2) we don't need to allocate any extra object (command is private and not exposed to end-users).
            lock (command)
            {
                new SendCommandPartitioned(command)
                {
                    Recipient = destinationAddress,
                    Headers = serializedHeaders,
                    Body = message.Body,
                    Priority = priority,
                    Now = now,
                    Visible = visible,
                    TtlSeconds = ttl,
                    GroupId = groupId
                }
                .ExecuteNonQuery();
            }

            return Task.CompletedTask;
        }

        string BuildReceiveSql() => $"{_table.Prefix}rebus_dequeue_{_table.Name}";

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _receiveBottleneck.Enter(cancellationToken))
            {
                var connection = context.GetConnection(_factory);

                using (var selectCommand = connection.CreateCommand())
                {
                    selectCommand.CommandText = _receiveSql;
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    selectCommand.Parameters.Add("recipientQueue", Address);
                    selectCommand.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());
                    selectCommand.Parameters.Add("output", OracleDbType.RefCursor, ParameterDirection.Output);
                    selectCommand.InitialLOBFetchSize = -1;
                    selectCommand.ExecuteNonQuery();

                    using (var reader = (selectCommand.Parameters["output"].Value as OracleRefCursor).GetDataReader())
                    {
                        if (!reader.Read()) return null;

                        var headers = (byte[])reader["headers"];
                        var body = (byte[])reader["body"];
                        var headersDictionary = HeaderSerializer.Deserialize(headers);

                        var message = new TransportMessage(headersDictionary, body);
                        
                        context.OnCompleted(() =>
                            MarkAsCompleted(message)
                        );

                        return message;
                    }
                }
            }
        }

        string BuildExpiredSql() => $"delete from {_table} where recipient = :recipient and expiration < :now";

        Task PerformExpiredMessagesCleanupCycle()
        {
            var stopwatch = Stopwatch.StartNew();

            using (var connection = _factory.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = _expiredSql;
                command.Parameters.Add("recipient", Address);
                command.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());

                int deletedRows = command.ExecuteNonQuery();

                connection.Complete();

                if (deletedRows > 0)
                {
                    _log.Info(
                        "Performed expired messages cleanup in {cleanupTime} - {deletedCount} expired messages with recipient {recipient} were deleted",
                        stopwatch.Elapsed, deletedRows, Address);
                }

                return Task.CompletedTask;
            }
        }

        /// <summary>Creates the necessary DB objects</summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                using (var connection = _factory.OpenRaw())
                {
                    if (connection.CreateRebusTransportPartitioned(_table))
                        _log.Info("Table {tableName} does not exist - it will be created now", _table);
                    else
                        _log.Info("Database already contains a table named {tableName} - will not create anything", _table);
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize Oracle transport schema with mesages table {_table}");
            }
        }

        /// <summary>Marks the specified message as completed</summary>
        public Task MarkAsCompleted(TransportMessage message)
        {
            var headerBytes = HeaderSerializer.Serialize(message.Headers);

            var completionSql = $@"update {_table} set status = :status where dbms_lob.compare(headers,:headers) = 0";

            using (var connection = _factory.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = completionSql;
                command.Parameters.Add("status", CompletedStatus);
                command.Parameters.Add("headers", headerBytes);

                int affectedRows = command.ExecuteNonQuery();

                connection.Complete();
                
                return Task.CompletedTask;
            }
        }

        /// <inheritdoc />
        // Note: IAsyncTask can be disposed multiple times without side-effects
        public void Dispose() => _expiredMessagesCleanupTask?.Dispose();
    }
}
