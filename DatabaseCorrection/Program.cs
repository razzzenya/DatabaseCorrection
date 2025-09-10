namespace DatabaseCorrection;

class Program
{
    static async Task Main()
    {
        var databaseComparison = new DatabaseComparison();
        databaseComparison.StartProcessing();
    }
}
