using System.Data;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using KpiApi.Models;

namespace KpiApi.Repositories;

/// <summary>
/// Executes optimised ClickHouse analytical queries against the mobility schema.
/// Each query is designed for columnar scan efficiency: filters push down to
/// partitions via toDate, aggregations use ClickHouse-native functions, and
/// FULL OUTER JOINs merge the orders + rider-pings fact tables per zone.
/// </summary>
public sealed class KpiRepository : IKpiRepository, IDisposable
{
    private readonly ClickHouseConnection _conn;
    private readonly KpiSettings _kpi;
    private readonly ILogger<KpiRepository> _log;

    public KpiRepository(
        IOptions<ClickHouseSettings> chSettings,
        IOptions<KpiSettings> kpiSettings,
        ILogger<KpiRepository> log)
    {
        _conn = new ClickHouseConnection(chSettings.Value.ConnectionString);
        _kpi = kpiSettings.Value;
        _log = log;
    }

    // ── 1. Zone Supply Gap (real-time) ───────────────────────────────────
    // Compares order volume vs unique active riders per zone within a
    // sliding window.  Gap = orders − riders; Pressure = orders / riders.
    public async Task<IReadOnlyList<SupplyGapDto>> GetSupplyGapAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH
                recent_orders AS (
                    SELECT zoneId, count() AS orders
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                ),
                recent_riders AS (
                    SELECT zoneId, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                )
            SELECT
                coalesce(o.zoneId, r.zoneId)          AS zoneId,
                coalesce(o.orders, 0)                  AS orders,
                coalesce(r.riders, 0)                  AS riders,
                toInt64(coalesce(o.orders, 0) - coalesce(r.riders, 0)) AS gap,
                round(coalesce(o.orders, 0) / greatest(coalesce(r.riders, 0), 1), 2) AS pressure
            FROM recent_orders o
            FULL OUTER JOIN recent_riders r ON o.zoneId = r.zoneId
            ORDER BY gap DESC";

        return await QueryAsync(sql, r => new SupplyGapDto(
            r.GetString(0),
            GetLong(r, 1),
            GetLong(r, 2),
            GetLong(r, 3),
            r.GetDouble(4)), ct);
    }

    // ── 2. Top Zones by Pressure ─────────────────────────────────────────
    // Highest demand-to-supply ratio — surfaces zones that need riders most.
    public async Task<IReadOnlyList<TopZoneDto>> GetTopZonesByPressureAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH
                recent_orders AS (
                    SELECT zoneId, count() AS orders
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                ),
                recent_riders AS (
                    SELECT zoneId, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                )
            SELECT
                coalesce(o.zoneId, r.zoneId)          AS zoneId,
                coalesce(o.orders, 0)                  AS orders,
                coalesce(r.riders, 0)                  AS riders,
                round(coalesce(o.orders, 0) / greatest(coalesce(r.riders, 0), 1), 2) AS pressure
            FROM recent_orders o
            FULL OUTER JOIN recent_riders r ON o.zoneId = r.zoneId
            ORDER BY pressure DESC
            LIMIT {_kpi.TopZonesLimit}";

        return await QueryAsync(sql, r => new TopZoneDto(
            r.GetString(0),
            GetLong(r, 1),
            GetLong(r, 2),
            r.GetDouble(3)), ct);
    }

    // ── 3. Rider Idle Detection ──────────────────────────────────────────
    // Finds riders dwelling in a single zone for longer than the threshold
    // with minimal GPS movement — likely idle in a high-demand area.
    // Movement is approximated via equirectangular projection from lat/lon.
    public async Task<IReadOnlyList<IdleRiderDto>> GetIdleRidersAsync(CancellationToken ct)
    {
        var sql = $@"
            SELECT
                riderId,
                zoneId,
                dateDiff('second', min(timestamp), max(timestamp)) AS dwellSeconds,
                round(
                    111320 * sqrt(
                        pow(max(lat) - min(lat), 2) +
                        pow(cos(radians(avg(lat))) * (max(lon) - min(lon)), 2)
                    ), 2
                ) AS movementMeters
            FROM mobility.rider_gps_pings
            WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes * 2} MINUTE
            GROUP BY riderId, zoneId
            HAVING dwellSeconds >= {_kpi.IdleDwellThresholdSeconds}
               AND movementMeters < {_kpi.IdleMovementThresholdMeters}
            ORDER BY dwellSeconds DESC";

        return await QueryAsync(sql, r => new IdleRiderDto(
            r.GetString(0),
            r.GetString(1),
            GetLong(r, 2),
            r.GetDouble(3)), ct);
    }

    // ── 4. Rider Utilization Rate ────────────────────────────────────────
    // Approximates speed between consecutive pings using equirectangular
    // distance / time-delta.  A ping with inter-speed > 3 m/s counts as
    // "active".  Utilization = active pings / total pings × 100.
    public async Task<IReadOnlyList<RiderUtilizationDto>> GetRiderUtilizationAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH movements AS (
                SELECT
                    riderId,
                    timestamp AS ts,
                    lat, lon,
                    lagInFrame(lat, 1, 0)
                        OVER (PARTITION BY riderId ORDER BY timestamp) AS prev_lat,
                    lagInFrame(lon, 1, 0)
                        OVER (PARTITION BY riderId ORDER BY timestamp) AS prev_lon,
                    lagInFrame(timestamp, 1, toDateTime64('1970-01-01', 3))
                        OVER (PARTITION BY riderId ORDER BY timestamp) AS prev_ts
                FROM mobility.rider_gps_pings
                WHERE timestamp >= now() - INTERVAL {_kpi.UtilizationWindowMinutes} MINUTE
            )
            SELECT
                riderId,
                round(
                    countIf(
                        111320.0 * sqrt(pow(lat - prev_lat, 2) + pow(cos(radians(lat)) * (lon - prev_lon), 2))
                        / greatest(dateDiff('second', prev_ts, ts), 1) > 3.0
                    ) * 100.0 / greatest(count(), 1),
                    1
                ) AS utilization
            FROM movements
            WHERE prev_ts > toDateTime64('1970-01-01', 3)
            GROUP BY riderId
            ORDER BY utilization DESC";

        return await QueryAsync(sql, r => new RiderUtilizationDto(
            r.GetString(0),
            r.GetDouble(1)), ct);
    }

    // ── 5. Orders per Zone per Minute (trend) ────────────────────────────
    // Time-series of order counts bucketed to 1-minute intervals for the
    // last hour — feeds the line chart on the dashboard.
    public async Task<IReadOnlyList<OrdersTrendDto>> GetOrdersTrendAsync(CancellationToken ct)
    {
        var sql = $@"
            SELECT
                zoneId,
                toStartOfMinute(createdAt) AS minute,
                toInt64(count())            AS orders
            FROM mobility.orders
            WHERE createdAt >= now() - INTERVAL {_kpi.TrendWindowMinutes} MINUTE
            GROUP BY zoneId, minute
            ORDER BY minute ASC, zoneId";

        return await QueryAsync(sql, r => new OrdersTrendDto(
            r.GetString(0),
            r.GetDateTime(1),
            GetLong(r, 2)), ct);
    }

    // ── 6. Historical Peak Gap per Zone ──────────────────────────────────
    // Over the last 24 h, finds the worst supply-demand gap per zone —
    // max(orders − riders) within each 1-minute bucket.
    public async Task<IReadOnlyList<PeakGapDto>> GetPeakGapAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH
                order_counts AS (
                    SELECT zoneId, toStartOfMinute(createdAt) AS minute, count() AS orders
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {_kpi.HistoricalWindowHours} HOUR
                    GROUP BY zoneId, minute
                ),
                rider_counts AS (
                    SELECT zoneId, toStartOfMinute(timestamp) AS minute, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {_kpi.HistoricalWindowHours} HOUR
                    GROUP BY zoneId, minute
                )
            SELECT
                coalesce(o.zoneId, r.zoneId)                                          AS zoneId,
                max(toInt64(coalesce(o.orders, 0)) - toInt64(coalesce(r.riders, 0)))   AS peakGap
            FROM order_counts o
            FULL OUTER JOIN rider_counts r ON o.zoneId = r.zoneId AND o.minute = r.minute
            GROUP BY zoneId
            ORDER BY peakGap DESC";

        return await QueryAsync(sql, r => new PeakGapDto(
            r.GetString(0),
            GetLong(r, 1)), ct);
    }

    // ── 7. Fulfillment Risk per Zone ─────────────────────────────────────
    // Risk = orders / riders.  A ratio > 1 means more orders than riders —
    // risk of delayed fulfilment.  Zones with zero riders get max risk.
    public async Task<IReadOnlyList<FulfillmentRiskDto>> GetFulfillmentRiskAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH
                recent_orders AS (
                    SELECT zoneId, count() AS orders
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                ),
                recent_riders AS (
                    SELECT zoneId, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                )
            SELECT
                coalesce(o.zoneId, r.zoneId) AS zoneId,
                coalesce(o.orders, 0)         AS orders,
                coalesce(r.riders, 0)         AS riders,
                round(coalesce(o.orders, 0) / greatest(coalesce(r.riders, 0), 1), 2) AS risk
            FROM recent_orders o
            FULL OUTER JOIN recent_riders r ON o.zoneId = r.zoneId
            ORDER BY risk DESC";

        return await QueryAsync(sql, r => new FulfillmentRiskDto(
            r.GetString(0),
            GetLong(r, 1),
            GetLong(r, 2),
            r.GetDouble(3)), ct);
    }

    // ── 8. Supply Reposition Recommendation ──────────────────────────────
    // Zones where the supply gap exceeds a configurable threshold — the gap
    // value itself is the number of additional riders needed to balance load.
    public async Task<IReadOnlyList<RepositionDto>> GetRepositionRecommendationsAsync(CancellationToken ct)
    {
        var sql = $@"
            WITH
                recent_orders AS (
                    SELECT zoneId, count() AS orders
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                ),
                recent_riders AS (
                    SELECT zoneId, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
                    GROUP BY zoneId
                )
            SELECT
                coalesce(o.zoneId, r.zoneId) AS zoneId,
                toInt64(coalesce(o.orders, 0) - coalesce(r.riders, 0)) AS neededRiders
            FROM recent_orders o
            FULL OUTER JOIN recent_riders r ON o.zoneId = r.zoneId
            WHERE coalesce(o.orders, 0) - coalesce(r.riders, 0) > {_kpi.RepositionGapThreshold}
            ORDER BY neededRiders DESC";

        return await QueryAsync(sql, r => new RepositionDto(
            r.GetString(0),
            GetLong(r, 1)), ct);
    }

    // ── 9. Rider Positions (for map heatmap) ────────────────────────────
    // Returns the latest GPS ping per rider within the realtime window.
    // LIMIT 1 BY riderId is a ClickHouse extension that keeps only the
    // first row per group after ORDER BY — perfect for "latest per rider".
    public async Task<IReadOnlyList<RiderPositionDto>> GetRiderPositionsAsync(CancellationToken ct)
    {
        var sql = $@"
            SELECT riderId, lat, lon, zoneId
            FROM mobility.rider_gps_pings
            WHERE timestamp >= now() - INTERVAL {_kpi.RealtimeWindowMinutes} MINUTE
            ORDER BY riderId, timestamp DESC
            LIMIT 1 BY riderId";

        return await QueryAsync(sql, r => new RiderPositionDto(
            r.GetString(0),
            r.GetDouble(1),
            r.GetDouble(2),
            r.GetString(3)), ct);
    }

    // ── 10. Surge Prediction ─────────────────────────────────────────────
    // Compares orders in the last 5 min vs the previous 5 min to detect
    // demand acceleration.  Surge score = acceleration × pressure, giving
    // a single sortable metric that captures both growth rate and severity.
    public async Task<IReadOnlyList<SurgePredictionDto>> GetSurgePredictionAsync(CancellationToken ct)
    {
        var window = _kpi.RealtimeWindowMinutes;
        var sql = $@"
            WITH
                orders_now AS (
                    SELECT zoneId, count() AS orders_5m
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {window} MINUTE
                    GROUP BY zoneId
                ),
                orders_prev AS (
                    SELECT zoneId, count() AS orders_prev
                    FROM mobility.orders
                    WHERE createdAt >= now() - INTERVAL {window * 2} MINUTE
                      AND createdAt <  now() - INTERVAL {window} MINUTE
                    GROUP BY zoneId
                ),
                riders_now AS (
                    SELECT zoneId, uniqExact(riderId) AS riders
                    FROM mobility.rider_gps_pings
                    WHERE timestamp >= now() - INTERVAL {window} MINUTE
                    GROUP BY zoneId
                )
            SELECT
                coalesce(n.zoneId, p.zoneId, r.zoneId)                       AS zoneId,
                toInt64(coalesce(n.orders_5m, 0))                             AS orders_5m,
                toInt64(coalesce(p.orders_prev, 0))                           AS orders_prev,
                toInt64(coalesce(r.riders, 0))                                AS riders,
                round(
                    if(coalesce(p.orders_prev, 0) > 0,
                       (toFloat64(coalesce(n.orders_5m, 0)) - coalesce(p.orders_prev, 0))
                           / coalesce(p.orders_prev, 0) * 100,
                       if(coalesce(n.orders_5m, 0) > 0, 100, 0)),
                    1)                                                        AS demand_acceleration,
                round(toFloat64(coalesce(n.orders_5m, 0))
                    / greatest(coalesce(r.riders, 0), 1), 2)                  AS pressure,
                round(
                    abs(if(coalesce(p.orders_prev, 0) > 0,
                        (toFloat64(coalesce(n.orders_5m, 0)) - coalesce(p.orders_prev, 0))
                            / coalesce(p.orders_prev, 0),
                        if(coalesce(n.orders_5m, 0) > 0, 1, 0)))
                    * (toFloat64(coalesce(n.orders_5m, 0))
                        / greatest(coalesce(r.riders, 0), 1))
                    * 10,
                    1)                                                        AS surge_score,
                greatest(toInt64(coalesce(n.orders_5m, 0))
                    - toInt64(coalesce(r.riders, 0)), 0)                      AS riders_needed
            FROM orders_now n
            FULL OUTER JOIN orders_prev p ON n.zoneId = p.zoneId
            FULL OUTER JOIN riders_now  r ON coalesce(n.zoneId, p.zoneId) = r.zoneId
            ORDER BY surge_score DESC";

        return await QueryAsync(sql, r => new SurgePredictionDto(
            r.GetString(0),
            GetLong(r, 1),
            GetLong(r, 2),
            GetLong(r, 3),
            r.GetDouble(4),
            r.GetDouble(5),
            r.GetDouble(6),
            GetLong(r, 7)), ct);
    }

    // ── 11. Rider List ──────────────────────────────────────────────────
    // Distinct rider IDs active in the last 24h — populates the rider selector.
    public async Task<IReadOnlyList<string>> GetRiderListAsync(CancellationToken ct)
    {
        var sql = $@"
            SELECT DISTINCT riderId
            FROM mobility.rider_gps_pings
            WHERE timestamp >= now() - INTERVAL {_kpi.HistoricalWindowHours} HOUR
            ORDER BY riderId";

        return await QueryAsync(sql, r => r.GetString(0), ct);
    }

    // ── 12. Rider Route Reconstruction ───────────────────────────────────
    // Returns every GPS ping for a specific rider, ordered chronologically.
    // Accepts optional from/to bounds; defaults to last HistoricalWindowHours.
    // Computes inter-ping distance (equirectangular) and speed so the
    // frontend can color-code movement vs stops.
    public async Task<IReadOnlyList<RiderRoutePointDto>> GetRiderRouteAsync(
        string riderId, DateTime? from, DateTime? to, CancellationToken ct)
    {
        var timeFilter = from.HasValue
            ? $"timestamp >= '{from.Value:yyyy-MM-dd HH:mm:ss}'"
            : $"timestamp >= now() - INTERVAL {_kpi.HistoricalWindowHours} HOUR";

        if (to.HasValue)
            timeFilter += $" AND timestamp <= '{to.Value:yyyy-MM-dd HH:mm:ss}'";

        var sql = $@"
            SELECT
                riderId, lat, lon, zoneId, timestamp,
                round(
                    111320.0 * sqrt(
                        pow(lat - lagInFrame(lat, 1, lat)
                            OVER (ORDER BY timestamp), 2) +
                        pow(cos(radians(lat)) *
                            (lon - lagInFrame(lon, 1, lon)
                                OVER (ORDER BY timestamp)), 2)
                    ), 2
                ) AS distFromPrev,
                round(
                    111320.0 * sqrt(
                        pow(lat - lagInFrame(lat, 1, lat)
                            OVER (ORDER BY timestamp), 2) +
                        pow(cos(radians(lat)) *
                            (lon - lagInFrame(lon, 1, lon)
                                OVER (ORDER BY timestamp)), 2)
                    ) / greatest(dateDiff('second',
                        lagInFrame(timestamp, 1, timestamp)
                            OVER (ORDER BY timestamp),
                        timestamp), 1),
                    2
                ) AS speedMps
            FROM mobility.rider_gps_pings
            WHERE riderId = '{riderId}'
              AND {timeFilter}
            ORDER BY timestamp ASC";

        return await QueryAsync(sql, r => new RiderRoutePointDto(
            r.GetString(0),
            r.GetDouble(1),
            r.GetDouble(2),
            r.GetString(3),
            r.GetDateTime(4),
            r.GetDouble(6),
            r.GetDouble(5)), ct);
    }

    // ClickHouse count()/uniqExact() return UInt64; coalesce preserves that.
    // IDataReader.GetInt64 expects signed Int64 and throws on UInt64.
    private static long GetLong(IDataReader r, int ordinal) =>
        Convert.ToInt64(r.GetValue(ordinal));

    // ── Shared helper ────────────────────────────────────────────────────
    private async Task<IReadOnlyList<T>> QueryAsync<T>(
        string sql, Func<IDataReader, T> map, CancellationToken ct)
    {
        var results = new List<T>();
        try
        {
            await using var cmd = _conn.CreateCommand();
            cmd.CommandText = sql;

            await _conn.OpenAsync(ct);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            while (await reader.ReadAsync(ct))
                results.Add(map(reader));
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "ClickHouse query failed: {Sql}", sql[..Math.Min(sql.Length, 120)]);
            throw;
        }
        finally
        {
            if (_conn.State != ConnectionState.Closed)
                await _conn.CloseAsync();
        }

        return results;
    }

    public void Dispose() => _conn.Dispose();
}
