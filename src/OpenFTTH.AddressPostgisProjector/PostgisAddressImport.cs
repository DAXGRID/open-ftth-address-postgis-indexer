using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;

namespace OpenFTTH.AddressPostgisProjector;

internal record OfficialAccessAddress
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

internal record OfficialUnitAddress
{
    public Guid Id { get; init; }
    public Guid AccessAddressId { get; init; }
    public string Status { get; init; }
    public string FloorName { get; init; }
    public string SuitName { get; init; }
    public string UnitAddressExternalId { get; init; }
    public string AccessAddressExternalId { get; init; }
    public bool Deleted { get; init; }

    public OfficialUnitAddress(
        Guid id,
        Guid accessAddressId,
        string status,
        string floorName,
        string suitName,
        string unitAddressExternalId,
        string accessAddressExternalId,
        bool deleted)
    {
        Id = id;
        AccessAddressId = accessAddressId;
        Status = status;
        FloorName = floorName;
        SuitName = suitName;
        UnitAddressExternalId = unitAddressExternalId;
        AccessAddressExternalId = accessAddressExternalId;
        Deleted = deleted;
    }
}

internal class PostgisAddressImport : IPostgisAddressImport
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
        using var conn = new NpgsqlConnection(_setting.PostgisConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);
        using var trans = await conn.BeginTransactionAsync().ConfigureAwait(false);

        _logger.LogInformation("Truncate access address table.");
        await TruncateTable("location.official_access_address", conn)
            .ConfigureAwait(false);

        _logger.LogInformation("Starting import access addresses.");
        await InsertOfficalAccessAddresses(projection, conn).ConfigureAwait(false);
        _logger.LogInformation("Finished import access addresses.");

        await trans.CommitAsync().ConfigureAwait(false);
    }

    private static async Task TruncateTable(string tableName, NpgsqlConnection conn)
    {
        using var truncateCmd = new NpgsqlCommand($"truncate table {tableName}", conn);
        await truncateCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task InsertOfficalAccessAddresses(
        AddressPostgisProjection projection,
        NpgsqlConnection conn)
    {
        var query = @"
                COPY location.official_access_address (
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

        using var writer = conn.BeginBinaryImport(query);

        foreach (var pAddress in projection.IdToAddress.Values)
        {
            var postCode = projection.IdToPostCode[pAddress.PostCodeId];
            var road = projection.IdToRoad[pAddress.RoadId];

            var address = MapAccessAddress(postCode, road, pAddress);
            var point = new Point(address.NorthCoordinate, address.EastCoordinate)
            {
                SRID = 25832
            };

            await writer.WriteRowAsync(
                default,
                address.Id,
                wkbWriter.Write(point),
                address.Status.ToString(),
                address.HouseNumber,
                address.RoadCode,
                address.RoadName,
                address.TownName ?? "",
                address.PostDistrictCode,
                address.PostDistrictName,
                address.MunicipalCode,
                address.AccessAdddressExternalId,
                address.RoadExternalId,
                address.PlotExternalId ?? "")
                .ConfigureAwait(false);
        }

        await writer.CompleteAsync().ConfigureAwait(false);
    }

    private static OfficialAccessAddress MapAccessAddress(
        PostCode postCode,
        Road road,
        AccessAddress address)
    {
        return new OfficialAccessAddress(
            id: address.Id,
            municipalCode: address.MunicipalCode,
            status: address.Status.ToString(),
            roadCode: address.RoadCode,
            houseNumber: address.HouseNumber,
            postDistrictCode: postCode.Code,
            postDistrictName: postCode.Name,
            eastCoordinate: address.EastCoordinate,
            northCoordinate: address.NorthCoordinate,
            accessAdddressExternalId: address.OfficialId ?? address.Id.ToString(),
            townName: address.TownName,
            plotExternalId: address.PlotId,
            roadExternalId: road.OfficialId,
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
