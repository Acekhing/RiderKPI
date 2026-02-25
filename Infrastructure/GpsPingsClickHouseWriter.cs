using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using KpiApi.Models;
using Microsoft.Extensions.Options;

namespace KpiApi.Infrastructure;

public sealed class GpsPingsClickHouseWriter : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<GpsPingsClickHouseWriter> _logger;

    private static readonly string[] ColumnNames =
    {
        "id", "eventId", "riderId", "lat", "lon",
        "riderPhone", "stationId", "zoneId", "timestamp"
    };

    public GpsPingsClickHouseWriter(
        IOptions<ClickHouseSettings> settings,
        ILogger<GpsPingsClickHouseWriter> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(settings.Value.ConnectionString);
    }

    public async Task BulkInsertAsync(IReadOnlyList<RiderGpsPing> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        using var bulkCopy = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "mobility.rider_gps_pings",
            ColumnNames = ColumnNames,
            BatchSize = batch.Count,
            MaxDegreeOfParallelism = 1
        };

        var rows = batch.Select(p => new object[]
        {
            p.Id,
            p.EventId,
            p.RiderId,
            p.Lat,
            p.Lon,
            p.RiderPhone,
            p.StationId,
            p.ZoneId,
            p.Timestamp
        });

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(rows);

        _logger.LogDebug("Inserted {Count} GPS pings into ClickHouse", batch.Count);
    }

    public void Dispose() => _connection.Dispose();
}
