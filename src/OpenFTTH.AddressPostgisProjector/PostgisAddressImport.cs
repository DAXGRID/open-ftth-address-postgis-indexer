using Npgsql;

namespace OpenFTTH.AddressPostgisProjector;

internal class PostgisAddressImport : IPostgisAddressImport
{
    private readonly Setting _setting;

    public PostgisAddressImport(Setting setting)
    {
        _setting = setting;
    }

    public void Init()
    {
        var sqlScript = File.ReadAllText(GetRootPath("Sql/location_schema.sql"));
        using var connection = new NpgsqlConnection(_setting.PostgisConnectionString);
        using var cmd = new NpgsqlCommand(sqlScript, connection);
        connection.Open();
        cmd.ExecuteNonQuery();
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
