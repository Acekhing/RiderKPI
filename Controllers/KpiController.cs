using Microsoft.AspNetCore.Mvc;
using KpiApi.Services;

namespace KpiApi.Controllers;

[ApiController]
[Route("api/kpi")]
[Produces("application/json")]
public sealed class KpiController : ControllerBase
{
    private readonly IKpiService _svc;

    public KpiController(IKpiService svc) => _svc = svc;

    /// <summary>Real-time supply vs demand per zone with gap and pressure ratio.</summary>
    [HttpGet("supply-gap")]
    public async Task<IActionResult> SupplyGap(CancellationToken ct) =>
        Ok(await _svc.GetSupplyGapAsync(ct));

    /// <summary>Zones ranked by highest demand-to-supply pressure.</summary>
    [HttpGet("top-zones")]
    public async Task<IActionResult> TopZones(CancellationToken ct) =>
        Ok(await _svc.GetTopZonesByPressureAsync(ct));

    /// <summary>Riders dwelling > 5 min with minimal movement in a zone.</summary>
    [HttpGet("idle-riders")]
    public async Task<IActionResult> IdleRiders(CancellationToken ct) =>
        Ok(await _svc.GetIdleRidersAsync(ct));

    /// <summary>Per-rider utilization: % of pings where speed exceeds 3 m/s.</summary>
    [HttpGet("rider-utilization")]
    public async Task<IActionResult> RiderUtilization(CancellationToken ct) =>
        Ok(await _svc.GetRiderUtilizationAsync(ct));

    /// <summary>Orders per zone bucketed to 1-minute intervals over the last hour.</summary>
    [HttpGet("orders-trend")]
    public async Task<IActionResult> OrdersTrend(CancellationToken ct) =>
        Ok(await _svc.GetOrdersTrendAsync(ct));

    /// <summary>Historical worst-case supply gap per zone (last 24 h).</summary>
    [HttpGet("peak-gap")]
    public async Task<IActionResult> PeakGap(CancellationToken ct) =>
        Ok(await _svc.GetPeakGapAsync(ct));

    /// <summary>Fulfilment risk: orders / riders per zone (ratio > 1 = risk).</summary>
    [HttpGet("fulfillment-risk")]
    public async Task<IActionResult> FulfillmentRisk(CancellationToken ct) =>
        Ok(await _svc.GetFulfillmentRiskAsync(ct));

    /// <summary>Zones where gap > threshold — recommended rider repositioning.</summary>
    [HttpGet("reposition")]
    public async Task<IActionResult> Reposition(CancellationToken ct) =>
        Ok(await _svc.GetRepositionRecommendationsAsync(ct));

    /// <summary>Latest GPS position per rider — feeds the map heatmap.</summary>
    [HttpGet("rider-positions")]
    public async Task<IActionResult> RiderPositions(CancellationToken ct) =>
        Ok(await _svc.GetRiderPositionsAsync(ct));

    /// <summary>Surge prediction: demand acceleration × pressure per zone.</summary>
    [HttpGet("surge")]
    public async Task<IActionResult> Surge(CancellationToken ct) =>
        Ok(await _svc.GetSurgePredictionAsync(ct));

    /// <summary>List of distinct rider IDs active in last 24 h.</summary>
    [HttpGet("riders")]
    public async Task<IActionResult> Riders(CancellationToken ct) =>
        Ok(await _svc.GetRiderListAsync(ct));

    /// <summary>Full GPS route for a rider — chronological pings with speed and distance.
    /// Optional from/to (ISO 8601) to narrow the time window.</summary>
    [HttpGet("rider-route")]
    public async Task<IActionResult> RiderRoute(
        [FromQuery] string riderId,
        [FromQuery] DateTime? from,
        [FromQuery] DateTime? to,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(riderId))
            return BadRequest("riderId is required");
        return Ok(await _svc.GetRiderRouteAsync(riderId, from, to, ct));
    }
}
