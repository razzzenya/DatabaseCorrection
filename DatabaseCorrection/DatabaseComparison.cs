using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.Text.Json;
using System.Xml.Linq;

namespace DatabaseCorrection;

public class LayerInfo
{
    public string Name { get; set; } = string.Empty;
    public string DBName { get; set; } = string.Empty;
    public List<string> Fields { get; set; } = [];
    public Dictionary<string, StyleInfo> Styles { get; set; } = [];
}

public class StyleInfo
{
    public string Name { get; set; } = string.Empty;
    public List<string> GeometryTypes { get; set; } = [];
}

public class ReferenceLayersUnion
{
    public string Name { get; set; } = string.Empty;
    public List<string> Fields { get; set; } = [];
    public Dictionary<string, StyleInfo> Styles { get; set; } = [];
}


public class LayerReportRecord
{
    public required string LayerName { get; set; }
    public List<string>? Actions { get; set; } = [];
    public List<string>? FieldsToAdd { get; set; } = [];
    public List<string>? FieldsToRemove { get; set; } = [];
    public Dictionary<string, StyleInfo>? StylesToAdd { get; set; } = [];
    public Dictionary<string, StyleInfo>? StylesToRemove { get; set; } = [];
}

public class DatabaseComparison
{
    public List<ReferenceLayersUnion> LayersUnion { get; set; } = [];
    public List<LayerInfo> CurrentLayers { get; set; } = [];
    List<LayerReportRecord> Report { get; set; } = [];
    List<LayerInfo> ExceptLayers { get; set; } = [];

    public void StartProcessing()
    {
        var refConnStr = "terplan-layer-library-v2.gpkg";
        var curConnStr = "Pooling=false;Data Source=192.168.0.102,1433;Initial Catalog=Empty;User ID=sa;Password=samis;TrustServerCertificate=True";

        var refTables = GetTableNames(refConnStr, exclude: "layer_styles");
        var actualTables = GetTableNames(curConnStr, prefix: "tpAcual");
        var plannedTables = GetTableNames(curConnStr, prefix: "tpPlan");
        GenerateExceptLayersList();

        foreach (var actualTable in actualTables)
        {
            CurrentLayers.Add(new LayerInfo
            {
                Name = actualTable.Replace("tpAcual", ""),
                DBName = actualTable,
                Fields = GetTableFields(curConnStr, actualTable),
                Styles = GetStylesFromCurrentTable(curConnStr, actualTable)
            });
        }
        foreach (var plannedTable in plannedTables)
        {
            CurrentLayers.Add(new LayerInfo
            {
                Name = plannedTable.Replace("tpPlan", ""),
                DBName = plannedTable,
                Fields = GetTableFields(curConnStr, plannedTable),
                Styles = GetStylesFromCurrentTable(curConnStr, plannedTable)
            });
        }
        foreach (var refTable in refTables)
        {
            var layersUnion = LayersUnion.FirstOrDefault(l => l.Name == TrimAfterUnderscore(refTable));
            if (layersUnion == null) // Нет такого слоя в объединённом списке
            {
                layersUnion = new ReferenceLayersUnion
                {
                    Name = TrimAfterUnderscore(refTable),
                    Fields = GetTableFields(refConnStr, refTable).Except(["geom", ""]).ToList(),
                    Styles = GetStylesFromReferenceTable(refConnStr, refTable)
                };
                LayersUnion.Add(layersUnion);
            }
            else // Есть такой слой в объединённом списке
            {
                var refFields = GetTableFields(refConnStr, refTable);
                layersUnion.Fields = layersUnion.Fields.Union(refFields).Except(["geom", ""]).ToList(); // Объединение полей
                var refStyles = GetStylesFromReferenceTable(refConnStr, refTable);
                foreach (var kv in refStyles)
                {
                    if (layersUnion.Styles.TryGetValue(kv.Key, out StyleInfo? existingStyle)) // Если такой стиль уже есть, то объединяем типы геометрий
                    {
                        existingStyle.GeometryTypes = existingStyle.GeometryTypes.Union(kv.Value.GeometryTypes).ToList();
                        layersUnion.Styles[kv.Key] = existingStyle;
                    }
                    else
                    {
                        layersUnion.Styles[kv.Key] = kv.Value; // Добавление нового стиля
                    }
                }
            }
        }
        for (var i = 0; i < LayersUnion.Count; i++)
        {
            if (ExceptLayers.Any(l => l.Name == LayersUnion[i].Name))
            {
                foreach (var style in LayersUnion[i].Styles)
                {
                    if (!style.Value.GeometryTypes.Contains("Multipolygon") && !style.Value.GeometryTypes.Contains("Multiline"))
                    {
                        style.Value.GeometryTypes.Add("Multipolygon");
                        style.Value.GeometryTypes.Add("Multiline");
                    }
                }
            }
        }
        GenerateReport();
    }

    public void GenerateReport()
    {
        foreach (var curLayer in CurrentLayers)
        {
            var refLayer = LayersUnion.FirstOrDefault(l => l.Name == curLayer.Name);
            if (refLayer == null) // Слои, которые есть в CurrentLayers, но которых нет в ReferenceLayers
            {
                Report.Add(new LayerReportRecord
                {
                    LayerName = curLayer.DBName,
                    Actions = ["Remove layer"],
                    FieldsToAdd = [],
                    FieldsToRemove = [],
                    StylesToAdd = [],
                    StylesToRemove = []
                });
            }
        }
        foreach (var layerUnion in LayersUnion)
        {
            var curLayer = CurrentLayers.FirstOrDefault(l => l.Name == layerUnion.Name);
            if (curLayer == null) // Слой есть в референсной бд, но отсутствует в нашей
            {
                Report.Add(new LayerReportRecord
                {
                    LayerName = layerUnion.Name,
                    Actions = ["Add layer", "Add field(s)", "Add style(s)"],
                    FieldsToAdd = layerUnion.Fields,
                    FieldsToRemove = [],
                    StylesToAdd = layerUnion.Styles,
                    StylesToRemove = []
                });
            }
            else // Слой есть в обоих бд
            {
                var actions = new List<string>();
                var fieldsToAdd = layerUnion.Fields.Except(curLayer.Fields.Concat(["geom", ""])).ToList();
                var fieldsToRemove = curLayer.Fields.Except(layerUnion.Fields.Append("ID")).ToList();
                var stylesToAdd = new Dictionary<string, StyleInfo>();
                var stylesToRemove = new Dictionary<string, StyleInfo>();
                foreach (var kv in curLayer.Styles)
                {
                    if (!layerUnion.Styles.ContainsKey(kv.Key))
                    {
                        stylesToRemove[kv.Key] = new StyleInfo { Name = kv.Value.Name, GeometryTypes = kv.Value.GeometryTypes }; // Удаление стиля, если его нет в объединённых стилях ReferenceLayers
                    }
                }
                foreach (var kv in layerUnion.Styles)
                {
                    if (!curLayer.Styles.TryGetValue(kv.Key, out StyleInfo? style)) // Если нет такого ключа, то добавляем стиль
                    {
                        stylesToAdd[kv.Key] = new StyleInfo { Name = kv.Value.Name, GeometryTypes = layerUnion.Styles[kv.Key].GeometryTypes };
                    }
                    else
                    {
                        var refGeomTypes = kv.Value.GeometryTypes;
                        var curGeomTypes = style.GeometryTypes;
                        if (curGeomTypes.Contains("Multipoint") && !refGeomTypes.Contains("Multipoint"))
                        {
                            stylesToRemove[kv.Key] = new StyleInfo { Name = style.Name, GeometryTypes = ["Multipoint"] };
                        }
                        else if (!curGeomTypes.Contains("Multipoint") && refGeomTypes.Contains("Multipoint"))
                        {
                            stylesToAdd[kv.Key] = new StyleInfo { Name = style.Name, GeometryTypes = ["Multipoint"] };
                        }

                        if (curGeomTypes.Contains("Multiline") && curGeomTypes.Contains("Multipolygon"))
                        {
                            if (!refGeomTypes.Contains("Multiline") && !refGeomTypes.Contains("Multipolygon"))
                            {
                                if (stylesToRemove.TryGetValue(kv.Key, out var existingStyle))
                                {
                                    existingStyle.GeometryTypes.Add("Multiline");
                                    existingStyle.GeometryTypes.Add("Multipolygon");
                                }
                                else
                                {
                                    stylesToRemove[kv.Key] = new StyleInfo { Name = style.Name, GeometryTypes = ["Multiline", "Multipolygon"] };
                                }
                            }
                        }
                        else
                        {
                            var typesToAdd = new List<string>();
                            if (refGeomTypes.Contains("Multiline") && !refGeomTypes.Contains("Multipolygon"))
                            {
                                typesToAdd.Add("Multiline");
                            }
                            else if (!refGeomTypes.Contains("Multiline") && refGeomTypes.Contains("Multipolygon"))
                            {
                                typesToAdd.Add("Multipolygon");
                            }
                            else if(refGeomTypes.Contains("Multiline") && refGeomTypes.Contains("Multipolygon"))
                            {
                                typesToAdd.Add("Multiline");
                                typesToAdd.Add("Multipolygon");
                            }
                            if (typesToAdd.Count == 0) continue;
                            if (stylesToAdd.TryGetValue(kv.Key, out var existingStyle))
                            {
                                existingStyle.GeometryTypes.Union(typesToAdd);
                            }
                            else
                            {
                                stylesToAdd[kv.Key] = new StyleInfo { Name = style.Name, GeometryTypes = typesToAdd };
                            }

                        }
                    }
                }
                var record = new LayerReportRecord
                {
                    LayerName = curLayer.DBName
                };
                if (fieldsToAdd.Count != 0) actions.Add("Add field(s)");
                else fieldsToAdd = null;
                if (fieldsToRemove.Count != 0) actions.Add("Remove field(s)");
                else fieldsToRemove = null;
                if (stylesToAdd.Count != 0) actions.Add("Add style(s)");
                else stylesToAdd = null;
                if (stylesToRemove.Count != 0) actions.Add("Remove style(s)");
                else stylesToRemove = null;
                if (actions.Count == 0) continue; // Если нет действий, то не добавляем запись в отчёт
                record.Actions = actions;
                record.FieldsToAdd = fieldsToAdd;
                record.FieldsToRemove = fieldsToRemove;
                record.StylesToAdd = stylesToAdd;
                record.StylesToRemove = stylesToRemove;
                Report.Add(record);
            }
        }

        File.WriteAllText("LayerReport.json",
            JsonSerializer.Serialize(Report, new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault
            }));
    }

    public void GenerateExceptLayersList()
    {
        var layerNames = File.ReadAllLines("../../../Exceptions.txt").Select(l => l.Trim()).Where(l => l != "").ToList();
        foreach (var layerName in layerNames)
        {
            ExceptLayers.Add(new LayerInfo
            {
                Name = layerName,
                DBName = "",
                Fields = [],
                Styles = []
            });
        }
    }

    static List<string> GetTableNames(string connStr, string? prefix = null, string? exclude = null)
    {
        var tables = new List<string>();
        if (connStr.EndsWith(".gpkg"))
        {
            using var connection = new SqliteConnection($"Data Source={connStr}");
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT table_name FROM gpkg_contents";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(0);
                if (exclude != null && exclude.Contains(name)) continue;
                if (prefix != null && !name.StartsWith(prefix)) continue;
                tables.Add(name);
            }
        }
        else
        {
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
        }
        return tables;
    }

    static List<string> GetTableFields(string connStr, string tableName)
    {
        var fields = new List<string>();
        if (connStr.EndsWith(".gpkg") || connStr.EndsWith(".sqlite"))
        {
            using var connection = new SqliteConnection($"Data Source={connStr}");
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = $"PRAGMA table_info('{tableName}')";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                fields.Add(reader.GetString(1));
            }
        }
        else
        {
            using var connection = new SqlConnection(connStr);
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                fields.Add(reader.GetString(0));
            }
        }
        return fields;
    }

    static Dictionary<string, StyleInfo> GetStylesFromReferenceTable(string connStr, string tableName)
    {
        var styles = new Dictionary<string, StyleInfo>();
        using var connection = new SqliteConnection($"Data Source={connStr}");
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = $"SELECT styleQML FROM layer_styles WHERE f_table_name = '{tableName}'";
        using var reader = command.ExecuteReader();
        var geometryType = TrimBeforeUnderscore(tableName);
        if (reader.Read())
        {
            styles = ParseStyleFromReferenceTable(reader.GetString(0), geometryType);
        }
        return styles;
    }

    static Dictionary<string, StyleInfo> GetStylesFromCurrentTable(string connStr, string tableName)
    {
        var styles = new Dictionary<string, StyleInfo>();
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
            var styleString = reader.GetString(0);
            var style = ParseStyleFromCurrentTable(styleString);
            if (style.key == "") continue; // Стиль без []
            if (!styles.ContainsKey(style.key))
                styles.Add(style.key, style.styleInfo);
            else if (styles[style.key].GeometryTypes.Except(style.styleInfo.GeometryTypes).Any()) // Если есть новые типы геометрий, то добавляем их
                styles[style.key].GeometryTypes = styles[style.key].GeometryTypes.Union(style.styleInfo.GeometryTypes).ToList();
        }
        reader.Close();
        return styles;
    }

    static Dictionary<string, StyleInfo> ParseStyleFromReferenceTable(string styleQML, string geometryType)
    {
        var result = new Dictionary<string, StyleInfo>();
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
            result[value] = new StyleInfo { Name = name, GeometryTypes = [geometryType] };
        }
        return result;
    }

    static (string key, StyleInfo styleInfo) ParseStyleFromCurrentTable(string styleString)
    {
        var firstIndex = styleString.IndexOf('[');
        var secondIndex = styleString.IndexOf(']');
        if (firstIndex < 0 || secondIndex < 0 || secondIndex <= firstIndex)
            return ("", new StyleInfo { Name = "", GeometryTypes = [""] }); // Стиль без []
        var geometryTypes = styleString.Contains("Знак") ? ["Multipoint"] :
                           styleString.Contains("Контур") ? new List<string> { "Multiline", "Multipolygon" } : [""];
        return (styleString.Substring(firstIndex + 1, secondIndex - firstIndex - 1), new StyleInfo { Name = styleString.Substring(0, firstIndex).Trim(), GeometryTypes = geometryTypes });
    }

    static string TrimAfterUnderscore(string str)
    {
        var index = str.IndexOf('_');
        return index >= 0 ? str.Substring(0, index) : str;
    }

    static string TrimBeforeUnderscore(string str)
    {
        var index = str.IndexOf('_');
        return index >= 0 ? str.Substring(index + 1) : str;
    }
}
