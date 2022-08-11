namespace OpenFTTH.AddressPostgisProjector;

internal interface IPostgisAddressImport
{
    void Init();
    Task Import(AddressPostgisProjection projection);
}
