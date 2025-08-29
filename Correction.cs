using Microsoft.Data.SqlClient;
using System.Data.Common;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace DatabaseCorrection;

public class LayerInfo
{
    public string Name { get; set; } = string.Empty;
    public string DbName { get; set; } = string.Empty;
    public List<string> Fields { get; set; } = [];
    public List<string> Styles { get; set; } = [];
}

public class LayerReportRecord
{
    public required string LayerName { get; set; }
    public List<string> Actions { get; set; } = [];
    public List<string> FieldsToAdd { get; set; } = [];
    public List<string> FieldsToRemove { get; set; } = [];
}

public class Correction
{
    List<LayerInfo> CurrentLayers { get; set; } = [];
    List<LayerInfo> ReferenceLayers { get; set; } = [];
    List<LayerReportRecord> Report { get; set; } = [];

    public void StartProcessing()
    {
        var refConnStr = "Pooling=false;Data Source=192.168.0.109,1433;Initial Catalog=TerplanSymbology;User ID=sa;Password=samis;TrustServerCertificate=True";
        var curConnStr = "Pooling=false;Data Source=192.168.0.109,1433;Initial Catalog=Empty;User ID=sa;Password=samis;TrustServerCertificate=True";

        var refTables = GetTableNames(refConnStr, exclude: "layer_styles");
        var curTables = GetTableNames(curConnStr, prefix: "tpAcual");

        foreach (var curTable in curTables)
        {
            CurrentLayers.Add(new LayerInfo
            {
                Name = curTable.Replace("tpAcual", ""),
                DbName = curTable,
                Fields =GetTableFields(curConnStr, curTable)
            });
        }
        foreach (var refTable in refTables)
        {
            ReferenceLayers.Add(new LayerInfo
            {
                Name = TrimAfterUnderscore(refTable),
                DbName = refTable,
                Fields = GetTableFields(refConnStr, refTable)
            });
        }
        GenerateReport();
    }

    public void GenerateReport()
    {
        Console.WriteLine("Generating report...");
        foreach (var curLayer in CurrentLayers)
        {
            var refLayer = ReferenceLayers.FirstOrDefault(l => l.Name == curLayer.Name);
            if (refLayer == null) // Слои, которых нет в ReferenceLayers
            {
                Report.Add(new LayerReportRecord
                {
                    LayerName = curLayer.DbName,
                    Actions = ["Remove layer"],
                    FieldsToAdd = [],
                    FieldsToRemove = []
                });
            }
        }

        foreach (var refLayer in ReferenceLayers)
        {
            var curLayer = CurrentLayers.FirstOrDefault(l => l.Name == refLayer.Name);
            if (Report.FirstOrDefault(r => r.LayerName == refLayer.Name) != null) // Проверяем, не дублируется ли запись в отчёте, так как в ReferenceLayer могут быть слои с одинаковым именем, но разным суффиксом
                continue;
            if (curLayer == null) // Слои, которые есть в ReferenceLayers, но которых нет в CurrentLayers
            {
                Report.Add(new LayerReportRecord
                {
                    LayerName = refLayer.Name,
                    Actions = ["Add layer", "Add field(s)"],
                    FieldsToAdd = refLayer.Fields.Except(["GEOM"]).ToList(),
                    FieldsToRemove = []
                });
            }
            else // Слои, которые есть в обоих списках
            {
                var actions = new List<string>();
                var fieldsToAdd = refLayer.Fields.Except(curLayer.Fields.Append("GEOM")).ToList(); // Игнорируем поле GEOM при сравнении
                var fieldsToRemove = curLayer.Fields.Except(refLayer.Fields.Append("ID")).ToList(); // Игнорируем поле ID при сравнении
                if (fieldsToAdd.Count != 0) actions.Add("Add field(s)");
                if (fieldsToRemove.Count != 0) actions.Add("Remove field(s)");
                Report.Add(new LayerReportRecord
                {
                    LayerName = curLayer.DbName,
                    Actions = actions,
                    FieldsToAdd = fieldsToAdd,
                    FieldsToRemove = fieldsToRemove
                });
            }
        }
        File.WriteAllText("LayerReport.json", JsonSerializer.Serialize(Report, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine("Report generated: LayerReport.json");
    }

    static List<string> GetTableNames(string connStr, string? prefix = null, string? exclude = null)
    {
        var tables = new List<string>();
        using var connection = new SqlConnection(connStr);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            if (exclude != null && exclude.Contains(name)) continue;
            if (prefix != null && !name.StartsWith(prefix)) continue;
            tables.Add(name);
        }
        return tables;
    }

    static List<string> GetTableFields(string connStr, string tableName)
    {
        var fields = new List<string>();
        using var connection = new SqlConnection(connStr);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            fields.Add(reader.GetString(0));
        }
        return fields;
    }

    static string TrimAfterUnderscore(string str)
    {
        var index = str.IndexOf('_');
        return index >= 0 ? str.Substring(0, index) : str;
    }

    static string GetSuffixAfterUnderscore(string str)
    {
        var index = str.IndexOf('_');
        return index >= 0 ? str.Substring(index) : string.Empty;
    }
}