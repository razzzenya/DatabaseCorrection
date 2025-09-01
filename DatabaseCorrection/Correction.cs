using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Xml.Linq;

namespace DatabaseCorrection;

public class LayerInfo
{
    public string Name { get; set; } = string.Empty;
    public string DbName { get; set; } = string.Empty;
    public List<string> Fields { get; set; } = [];
    public Dictionary<string, string> Styles { get; set; } = [];
}

public class LayerReportRecord
{
    public required string LayerName { get; set; }
    public List<string> Actions { get; set; } = [];
    public List<string> FieldsToAdd { get; set; } = [];
    public List<string> FieldsToRemove { get; set; } = [];
    public Dictionary<string, string> StylesToAdd { get; set; } = [];
    public Dictionary<string, string> StylesToRemove { get; set; } = [];
}

public class Correction
{
    List<LayerInfo> CurrentLayers { get; set; } = [];
    List<LayerInfo> ReferenceLayers { get; set; } = [];
    Dictionary<string, Dictionary<string, string>> ReferenceStylesUnion { get; set; } = []; // Словарь для хранения объединённых стилей из ReferenceLayers
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
                Fields = GetTableFields(curConnStr, curTable),
                Styles = GetStylesFromCurrentTable(curConnStr, curTable)
            });
        }
        foreach (var refTable in refTables)
        {
            ReferenceLayers.Add(new LayerInfo
            {
                Name = TrimAfterUnderscore(refTable),
                DbName = refTable,
                Fields = GetTableFields(refConnStr, refTable),
                Styles = GetStylesFromReferenceTable(refConnStr, refTable)
            });
        }
        GetUnionOfReferenceStyles();
        GenerateReport();
    }

    public void GenerateReport()
    {
        Console.WriteLine("Generating report...");
        Console.Out.Flush();
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
                    FieldsToRemove = [],
                    StylesToAdd = [],
                    StylesToRemove = []
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
                    Actions = ["Add layer", "Add field(s)", "Add style(s)"],
                    FieldsToAdd = refLayer.Fields.Except(["GEOM"]).ToList(),
                    FieldsToRemove = [],
                    StylesToAdd = ReferenceStylesUnion[refLayer.Name],
                    StylesToRemove = []
                });
            }
            else // Слои, которые есть в обоих списках
            {
                var actions = new List<string>();
                var fieldsToAdd = refLayer.Fields.Except(curLayer.Fields.Append("GEOM")).ToList(); // Игнорируем поле GEOM при сравнении
                var fieldsToRemove = curLayer.Fields.Except(refLayer.Fields.Append("ID")).ToList(); // Игнорируем поле ID при сравнении
                var stylesToAdd = new Dictionary<string, string>();
                var stylesToRemove = new Dictionary<string, string>();

                foreach (var kv in curLayer.Styles)
                {
                    if(!ReferenceStylesUnion[refLayer.Name].ContainsKey(kv.Key))
                    {
                        stylesToRemove[kv.Key] = kv.Value; // Удаление стиля, если его нет в объединённых стилях ReferenceLayers
                    }
                }

                foreach (var kv in ReferenceStylesUnion[refLayer.Name])
                {
                    if (!curLayer.Styles.TryGetValue(kv.Key, out string? value)) // Если нет такого ключа, то добавляем стиль
                    {
                        stylesToAdd[kv.Key] = kv.Value;
                    }
                    else if(value != kv.Value)
                    {
                        stylesToAdd[kv.Key] = kv.Value; // Обновление стиля, если значение отличается
                        stylesToRemove[kv.Key] = value;
                    }
                }

                if (fieldsToAdd.Count != 0) actions.Add("Add field(s)");
                if (fieldsToRemove.Count != 0) actions.Add("Remove field(s)");
                if (stylesToAdd.Count != 0) actions.Add("Add style(s)");
                if (stylesToRemove.Count != 0) actions.Add("Remove style(s)");
                Report.Add(new LayerReportRecord
                {
                    LayerName = curLayer.DbName,
                    Actions = actions,
                    FieldsToAdd = fieldsToAdd,
                    FieldsToRemove = fieldsToRemove,
                    StylesToAdd = stylesToAdd,
                    StylesToRemove = stylesToRemove
                });
            }
        }
        File.WriteAllText("LayerReport.json", JsonSerializer.Serialize(Report, new JsonSerializerOptions { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }));
        Console.WriteLine("Report generated: LayerReport.json");
    }

    public void GetUnionOfReferenceStyles()
    {
        foreach (var layer in ReferenceLayers)
        {
            if (!ReferenceStylesUnion.ContainsKey(layer.Name))
            {
                ReferenceStylesUnion[layer.Name] = new(layer.Styles);
            }
            else
            {
                ReferenceStylesUnion[layer.Name] = ReferenceStylesUnion[layer.Name]
                                                    .Concat(layer.Styles)
                                                    .GroupBy(kv => kv.Key)
                                                    .ToDictionary(g => g.Key, g => g.Last().Value);
            }
        }
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

    static Dictionary<string, string> GetStylesFromReferenceTable(string connStr, string tableName)
    {
        var styles = new Dictionary<string, string>();
        using var connection = new SqlConnection(connStr);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = $"SELECT styleQML FROM layer_styles WHERE CAST(f_table_name AS varchar(max)) = '{tableName}'";
        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            styles = ParseStyleFromReferenceTable(reader.GetString(0));
        }
        return styles;
    }

    static Dictionary<string, string> GetStylesFromCurrentTable(string connStr, string tableName)
    {
        var styles = new Dictionary<string, string>();
        using var connection = new SqlConnection(connStr);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = $"SELECT LAYERID FROM INGEO_SEMTABLS WHERE CAST(TABLENAME AS varchar(max)) = '{tableName}'";
        var reader = command.ExecuteReader();
        var layerId = string.Empty;
        if (reader.Read())
        {
            layerId = reader.GetString(0);
        }
        command.CommandText = $"SELECT STYLENAME FROM INGEO_STYLES WHERE LAYERID = '{layerId}'"; 
        reader.Close();
        reader = command.ExecuteReader();
        while (reader.Read())
        {
            var style = ParseStyleFromCurrentTable(reader.GetString(0));
            if (style.key == "") continue; // Стиль без []
            if (!styles.ContainsKey(style.key))
                styles.Add(style.key, style.value);
        }
        reader.Close();
        return styles;
    }

    static Dictionary<string, string> ParseStyleFromReferenceTable(string styleQML)
    {
        var result = new Dictionary<string, string>();
        var doc = XDocument.Parse(styleQML);
        var field = doc.Descendants("field").FirstOrDefault(f => (string)f.Attribute("name") == "CLASSID");
        if (field == null)
            return result;
        var options = field.Descendants("editWidget")
        .Where(e => (string)e.Attribute("type") == "ValueMap")
        .Descendants("Option")
        .Where(o => o.Attribute("value") != null && o.Attribute("name") != null);

        foreach (var opt in options)
        {
            var value = opt.Attribute("value")?.Value ?? "";
            var name = opt.Attribute("name")?.Value ?? "";
            result[value] = name;
        }
        return result;
    }

    static (string key, string value) ParseStyleFromCurrentTable(string style)
    {
        var firstIndex = style.IndexOf('[');
        var secondIndex = style.IndexOf(']');
        if (firstIndex < 0 || secondIndex < 0 || secondIndex <= firstIndex)
            return (style, ""); // Стиль без []
        return (style.Substring(firstIndex + 1, secondIndex - firstIndex - 1), style.Substring(0, firstIndex).Trim());
    }

    static string TrimAfterUnderscore(string str)
    {
        var index = str.IndexOf('_');
        return index >= 0 ? str.Substring(0, index) : str;
    }
}