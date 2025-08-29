namespace DatabaseCorrection;

class Program
{
    static async Task Main()
    {
        var Correction = new Correction();
        Correction.StartProcessing();
    }
}
