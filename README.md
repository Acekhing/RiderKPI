# KPI API

A real-time analytics API for a mobility/rideshare platform, built with ASP.NET Core 8 and ClickHouse. It exposes 12 KPI endpoints covering supply-demand gaps, rider utilization, surge prediction, route tracking, and more.

## Tech Stack

- **Runtime:** .NET 8 / ASP.NET Core (Minimal API)
- **Database:** ClickHouse (via [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client))
- **Logging:** Serilog (console sink)
- **API Docs:** Swagger / OpenAPI (Swashbuckle)
- **Containerization:** Docker (multi-stage build)

## Project Structure

```
KpiApi/
├── Controllers/
│   └── KpiController.cs        # API endpoints
├── Services/
│   ├── IKpiService.cs           # Service interface
│   └── KpiService.cs            # Service implementation
├── Repositories/
│   ├── IKpiRepository.cs        # Repository interface
│   └── KpiRepository.cs         # ClickHouse queries
├── Models/
│   ├── KpiDtos.cs               # Response DTOs
│   └── KpiSettings.cs           # Configuration models
├── Program.cs                   # Application entry point & DI setup
├── appsettings.json             # Configuration
└── Dockerfile
```

## API Endpoints

All endpoints are prefixed with `/api/kpi`.

| Method | Path                | Description                                          |
|--------|---------------------|------------------------------------------------------|
| GET    | `/supply-gap`       | Real-time supply vs demand per zone                  |
| GET    | `/top-zones`        | Zones ranked by highest demand-to-supply pressure    |
| GET    | `/idle-riders`      | Riders dwelling > 5 min with minimal movement        |
| GET    | `/rider-utilization`| Per-rider utilization percentage                     |
| GET    | `/orders-trend`     | Orders per zone bucketed to 1-minute intervals       |
| GET    | `/peak-gap`         | Historical worst-case supply gap per zone            |
| GET    | `/fulfillment-risk` | Fulfillment risk (orders/riders ratio)               |
| GET    | `/reposition`       | Zones needing rider repositioning                    |
| GET    | `/rider-positions`  | Latest GPS position per rider                        |
| GET    | `/surge`            | Surge prediction (demand acceleration × pressure)    |
| GET    | `/riders`           | List of distinct rider IDs                           |
| GET    | `/rider-route`      | Full GPS route for a rider (optional time filters)   |

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- A running ClickHouse instance with the `mobility` database containing `orders` and `rider_gps_pings` tables

## Getting Started

### Configuration

Update the ClickHouse connection string in `appsettings.json`:

```json
{
  "ClickHouse": {
    "ConnectionString": "Host=localhost;Port=8123;Database=mobility"
  }
}
```

### Run Locally

```bash
dotnet restore
dotnet run
```

The API will be available at `http://localhost:5200` with Swagger UI at `http://localhost:5200/swagger`.

### Run with Docker

```bash
docker build -t kpi-api .
docker run -p 10000:10000 kpi-api
```

## Configuration Reference

Key settings in `appsettings.json`:

| Section        | Key                | Description                          |
|----------------|--------------------|--------------------------------------|
| `ClickHouse`   | `ConnectionString` | ClickHouse connection string         |
| `KpiSettings`  | `WindowMinutes`    | Time window for real-time queries    |
| `KpiSettings`  | `IdleThreshold`    | Threshold for idle rider detection   |
| `KpiSettings`  | `TopZoneLimit`     | Max zones returned by top-zones      |
| `CorsSettings` | `AllowedOrigins`   | Allowed CORS origins (frontend URL)  |
