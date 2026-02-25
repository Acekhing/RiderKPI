using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using KpiApi.Models;
using Microsoft.Extensions.Options;

namespace KpiApi.Infrastructure;

public sealed class OrdersClickHouseWriter : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<OrdersClickHouseWriter> _logger;

    private static readonly string[] ColumnNames = ["orderId", "zoneId", "createdAt"];

    public OrdersClickHouseWriter(
        IOptions<ClickHouseSettings> settings,
        ILogger<OrdersClickHouseWriter> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(settings.Value.ConnectionString);
    }

    public async Task BulkInsertAsync(IReadOnlyList<OrderEvent> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        using var bulkCopy = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "mobility.orders",
            ColumnNames = ColumnNames,
            BatchSize = batch.Count,
            MaxDegreeOfParallelism = 1
        };

        var rows = batch.Select(o => new object[]
        {
            o.OrderId,
            o.ZoneId,
            o.CreatedAt
        });

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(rows);

        _logger.LogDebug("Inserted {Count} orders into ClickHouse", batch.Count);
    }

    public void Dispose() => _connection.Dispose();
}
