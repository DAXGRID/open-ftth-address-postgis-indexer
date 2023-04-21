using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed record OfficialUnitAddress
{
    public Guid Id { get; init; }
    public Guid AccessAddressId { get; init; }
    public string Status { get; init; }
    public string? FloorName { get; init; }
    public string? SuitName { get; init; }
    public string? UnitAddresssExternalId { get; init; }
    public string? AccessAddressExternalId { get; init; }
    public bool Deleted { get; init; }

    public OfficialUnitAddress(
        Guid id,
        Guid accessAddressId,
        string status,
        string? floorName,
        string? suitName,
        string? unitAddresssExternalId,
        string? accessAddressExternalId,
        bool deleted)
    {
        Id = id;
        AccessAddressId = accessAddressId;
        Status = status;
        FloorName = floorName;
        SuitName = suitName;
        UnitAddresssExternalId = unitAddresssExternalId;
        AccessAddressExternalId = accessAddressExternalId;
        Deleted = deleted;
    }
}

internal sealed record OfficialAccessAddress
{
    public Guid Id { get; init; }
    public string MunicipalCode { get; init; }
    public string Status { get; init; }
    public string RoadCode { get; init; }
    public string HouseNumber { get; init; }
    public string PostDistrictCode { get; init; }
    public string PostDistrictName { get; init; }
    public double EastCoordinate { get; init; }
    public double NorthCoordinate { get; init; }
    public string AccessAdddressExternalId { get; init; }
    public string? TownName { get; init; }
    public string? PlotExternalId { get; init; }
    public string RoadExternalId { get; init; }
    public string RoadName { get; init; }
    public bool Deleted { get; init; }

    public OfficialAccessAddress(
        Guid id,
        string municipalCode,
        string status,
        string roadCode,
        string houseNumber,
        string postDistrictCode,
        string postDistrictName,
        double eastCoordinate,
        double northCoordinate,
        string accessAdddressExternalId,
        string? townName,
        string? plotExternalId,
        string roadExternalId,
        string roadName,
        bool deleted)
    {
        Id = id;
        MunicipalCode = municipalCode;
        Status = status;
        RoadCode = roadCode;
        HouseNumber = houseNumber;
        PostDistrictCode = postDistrictCode;
        PostDistrictName = postDistrictName;
        EastCoordinate = eastCoordinate;
        NorthCoordinate = northCoordinate;
        AccessAdddressExternalId = accessAdddressExternalId;
        TownName = townName;
        PlotExternalId = plotExternalId;
        RoadExternalId = roadExternalId;
        RoadName = roadName;
        Deleted = deleted;
    }
}

internal sealed class PostgisAddressImport : IPostgisAddressImport
{
    private readonly Setting _setting;
    private readonly ILogger<PostgisAddressImport> _logger;

    public PostgisAddressImport(Setting setting, ILogger<PostgisAddressImport> logger)
    {
        _setting = setting;
        _logger = logger;
    }

    public void Init()
    {
        var sqlScript = File.ReadAllText(GetRootPath("Sql/location_schema.sql"));
        using var connection = new NpgsqlConnection(_setting.PostgisConnectionString);
        using var cmd = new NpgsqlCommand(sqlScript, connection);
        connection.Open();
        cmd.ExecuteNonQuery();
    }

    public async Task Import(AddressPostgisProjection projection)
    {
        _logger.LogInformation("Truncate address bulk tables.");

        await TruncateTable("location.official_access_address_bulk")
            .ConfigureAwait(false);

        await TruncateTable("location.official_unit_address_bulk")
            .ConfigureAwait(false);

        _logger.LogInformation("Finsihed truncate address bulk tables.");

        _logger.LogInformation("Starting importing addresses.");

        var insertAccessAddressesTask = InsertOfficalAccessAddresses(projection);
        var insertUnitAddressesTask = InsertOfficalUnitAddress(projection);

        await Task.WhenAll(insertAccessAddressesTask, insertUnitAddressesTask)
            .ConfigureAwait(false);

        _logger.LogInformation("Finished importing addresses.");

        _logger.LogInformation("Starting refreshing views.");

        var refreshAccessAddressTask = RefreshMaterializedView(
            "location.official_access_address");

        var refreshUnitAddressTask = RefreshMaterializedView(
            "location.official_unit_address");

        await Task.WhenAll(refreshAccessAddressTask, refreshUnitAddressTask)
            .ConfigureAwait(false);

        _logger.LogInformation("Finished updating views.");
    }

    private async Task RefreshMaterializedView(string viewName)
    {
        using var conn = new NpgsqlConnection(_setting.PostgisConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);

        using var truncateCmd = new NpgsqlCommand(
           $"REFRESH MATERIALIZED VIEW CONCURRENTLY {viewName}", conn);
        truncateCmd.CommandTimeout = 60 * 30;

        await truncateCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private async Task TruncateTable(string tableName)
    {
        using var conn = new NpgsqlConnection(_setting.PostgisConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);

        using var truncateCmd = new NpgsqlCommand(
            $"truncate table {tableName}", conn);

        await truncateCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private async Task InsertOfficalAccessAddresses(AddressPostgisProjection projection)
    {
        var query = @"
                COPY location.official_access_address_bulk (
                    id,
                    coord,
                    status,
                    house_number,
                    road_code,
                    road_name,
                    town_name,
                    post_district_code,
                    post_district_name,
                    municipal_code,
                    access_address_external_id,
                    road_external_id,
                    plot_external_id
                ) FROM STDIN (FORMAT BINARY)";

        var wkbWriter = new WKBWriter(ByteOrder.LittleEndian, true, false, false);

        using var conn = new NpgsqlConnection(_setting.PostgisConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);

        using var writer = conn.BeginBinaryImport(query);

        foreach (var (key, pAddress) in projection.IdToAccessAddress)
        {
            var postCode = projection.IdToPostCode[pAddress.PostCodeId];
            var road = projection.IdToRoad[pAddress.RoadId];

            var address = MapAccessAddress(key, postCode, road, pAddress);
            var point = new Point(address.EastCoordinate, address.NorthCoordinate)
            {
                SRID = 25832
            };

            await writer.WriteRowAsync(
                default,
                address.Id,
                wkbWriter.Write(point),
                address.Status,
                address.HouseNumber,
                address.RoadCode,
                address.RoadName,
                address.TownName is null
                ? DBNull.Value : address.TownName,
                address.PostDistrictCode,
                address.PostDistrictName,
                address.MunicipalCode,
                address.AccessAdddressExternalId,
                address.RoadExternalId,
                address.PlotExternalId is null
                ? DBNull.Value : address.PlotExternalId
            ).ConfigureAwait(false);
        }

        await writer.CompleteAsync().ConfigureAwait(false);
    }

    private async Task InsertOfficalUnitAddress(AddressPostgisProjection projection)
    {
        var query = @"
                COPY location.official_unit_address_bulk (
                    id,
                    access_address_id,
                    status,
                    floor_name,
                    suit_name,
                    unit_address_external_id,
                    access_address_external_id,
                    deleted
                ) FROM STDIN (FORMAT BINARY)";

        using var conn = new NpgsqlConnection(_setting.PostgisConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);

        using var writer = conn.BeginBinaryImport(query);
        foreach (var (k, pUnitAddress) in projection.IdToUnitAddress)
        {
            var accessAddressExternalId = projection
                .IdToAccessAddress[pUnitAddress.AccessAddressId].ExternalId;

            var unitAddress = MapUnitAddress(k, accessAddressExternalId, pUnitAddress);

            await writer.WriteRowAsync(
                default,
                unitAddress.Id,
                unitAddress.AccessAddressId,
                unitAddress.Status,
                unitAddress.FloorName is null
                ? DBNull.Value : unitAddress.FloorName,
                unitAddress.SuitName is null
                ? DBNull.Value : unitAddress.SuitName,
                unitAddress.UnitAddresssExternalId is null
                ? DBNull.Value : unitAddress.UnitAddresssExternalId,
                unitAddress.AccessAddressExternalId is null
                ? DBNull.Value : unitAddress.AccessAddressExternalId,
                unitAddress.Deleted).ConfigureAwait(false);
        }

        await writer.CompleteAsync().ConfigureAwait(false);
    }

    private static OfficialUnitAddress MapUnitAddress(
            Guid id,
            string? accessAddressExternalId,
            UnitAddress address)
    {
        return new OfficialUnitAddress(
            id: id,
            accessAddressId: address.AccessAddressId,
            status: address.Status,
            floorName: address.FloorName,
            suitName: address.SuitName,
            unitAddresssExternalId: address.ExternalId,
            accessAddressExternalId: accessAddressExternalId,
            deleted: address.Deleted);
    }

    private static OfficialAccessAddress MapAccessAddress(
        Guid id,
        PostCode postCode,
        Road road,
        AccessAddress address)
    {
        return new OfficialAccessAddress(
            id: id,
            municipalCode: address.MunicipalCode,
            status: address.Status,
            roadCode: address.RoadCode,
            houseNumber: address.HouseNumber,
            postDistrictCode: postCode.Code,
            postDistrictName: postCode.Name,
            eastCoordinate: address.EastCoordinate,
            northCoordinate: address.NorthCoordinate,
            accessAdddressExternalId: address.ExternalId ?? id.ToString(),
            townName: address.TownName,
            plotExternalId: address.PlotId,
            roadExternalId: road.ExternalId,
            roadName: road.Name,
            deleted: address.Deleted);
    }

    private static string GetRootPath(string filePath)
    {
        var absolutePath = Path.IsPathRooted(filePath)
            ? filePath
            : Path.GetRelativePath(Directory.GetCurrentDirectory(), filePath);

        return File.Exists(absolutePath)
            ? absolutePath
            : throw new ArgumentException($"Could not find file at path: {absolutePath}");
    }
}
