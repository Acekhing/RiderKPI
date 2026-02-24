namespace KpiApi.Models;

public sealed record SupplyGapDto(
    string ZoneId,
    long Orders,
    long Riders,
    long Gap,
    double Pressure);

public sealed record TopZoneDto(
    string ZoneId,
    long Orders,
    long Riders,
    double Pressure);

public sealed record IdleRiderDto(
    string RiderId,
    string ZoneId,
    long DwellSeconds,
    double MovementMeters);

public sealed record RiderUtilizationDto(
    string RiderId,
    double Utilization);

public sealed record OrdersTrendDto(
    string ZoneId,
    DateTime Minute,
    long Orders);

public sealed record PeakGapDto(
    string ZoneId,
    long PeakGap);

public sealed record FulfillmentRiskDto(
    string ZoneId,
    long Orders,
    long Riders,
    double Risk);

public sealed record RepositionDto(
    string ZoneId,
    long NeededRiders);

public sealed record RiderPositionDto(
    string RiderId,
    double Lat,
    double Lon,
    string ZoneId);

public sealed record RiderRoutePointDto(
    string RiderId,
    double Lat,
    double Lon,
    string ZoneId,
    DateTime Timestamp,
    double SpeedMps,
    double DistanceFromPrev);

public sealed record SurgePredictionDto(
    string ZoneId,
    long Orders5m,
    long OrdersPrev,
    long Riders,
    double DemandAcceleration,
    double Pressure,
    double SurgeScore,
    long RidersNeeded);
