using System.Data.SqlClient;
using System.IO.Compression;
using System.Text;
using AIGPTMigrateProcedureMSToPG.Models;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace AIGPTMigrateProcedureMSToPG.Services;

public class ProcedureService
{
    private readonly HttpClient _httpClient;
    private readonly string _apiKey;
    private readonly string _connectionString;

    public ProcedureService(HttpClient httpClient, IOptions<OpenAIOptions> options, string connectionString)
    {
        _httpClient = httpClient;
        _httpClient.BaseAddress = new Uri(options.Value.BaseUrl);
        _apiKey = options.Value.ApiKey;
        _connectionString = connectionString;
    }

    public async Task<List<(string Name, string Definition)>> GetStoredProceduresFromMssql()
    {
        var procedures = new List<(string Name, string Definition)>();

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        var query = @"
                SELECT s.name AS schema_name, 
                       o.name AS procedure_name
                FROM sys.objects o
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE o.type = 'P'
                order by s.name, o.name";


        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            string schemaName = reader.GetString(0);
            string procedureName = reader.GetString(1);
            
            string fullProcedureName = $"{schemaName}.{procedureName}";

            string procedureDefinition = await GetProcedureDefinition(connection, fullProcedureName);
            procedures.Add((fullProcedureName, procedureDefinition));
        }

        return procedures;
    }
    
    private async Task<string> GetProcedureDefinition(SqlConnection connection, string procedureName)
    {
        var definitionBuilder = new StringBuilder();

        using var command = new SqlCommand($"sp_helptext '{procedureName}'", connection);
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            definitionBuilder.AppendLine(reader.GetString(0));
        }

        return definitionBuilder.ToString();
    }

    public async Task<string> ConvertLongProcedureToPgSql(string procedureText)
    {
        var fullResult = new StringBuilder();
        int chunkSize = 500;

        var procedureLines = procedureText.Split('\n');
        var chunks = procedureLines.Select((line, index) => new { line, index })
            .GroupBy(x => x.index / chunkSize)
            .Select(g => string.Join("\n", g.Select(x => x.line)))
            .ToList();

        for (int i = 0; i < chunks.Count; i++)
        {
            bool isComplete = false;
            string chunk = chunks[i];

            while (!isComplete)
            {
                string content = i == 0
                    ? $"Convert the following MSSQL stored procedure to PostgreSQL (Part {i + 1} of {chunks.Count}):\n{chunk}\n" +
                      "Please follow these detailed instructions:\n" +
                      "1. Replace temporary tables created with `#` in MSSQL with `CREATE TEMP TABLE` as PostgreSQL does not support `#`.\n" +
                      "2. Use `TRUE` and `FALSE` for boolean values instead of `1` and `0`.\n" +
                      "3. Replace `TRUNCATE TABLE` for temporary tables with `DELETE FROM`, as `TRUNCATE` can cause locking issues in PostgreSQL.\n" +
                      "4. Remove the `dbo` prefix or replace it with `public` as appropriate.\n" +
                      "5. If transactions are used, translate `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK` to `BEGIN`, `COMMIT`, and `ROLLBACK` statements.\n" +
                      "6. Use `EXCEPTION` blocks in PostgreSQL for error handling instead of `TRY...CATCH` blocks in MSSQL.\n" +
                      "7. If `OUTPUT` parameters are used, replace them with `RETURNS TABLE` or `RETURN QUERY` in PostgreSQL.\n" +
                      "8. Replace `EXEC` statements with `CALL` when calling stored procedures and ensure functions are used directly in queries.\n" +
                      "9. Translate references to MSSQL system tables (like `sys.objects`) to PostgreSQL equivalents (like `pg_catalog.pg_class`).\n" +
                      "10. Translate `RAISEERROR` and `PRINT` statements to PostgreSQL `RAISE` with appropriate severity levels (`NOTICE`, `WARNING`, `EXCEPTION`).\n" +
                      "11. If `CROSS JOIN` is used, ensure it is translated explicitly in PostgreSQL syntax rather than implicit joins (e.g., `FROM table1, table2`).\n" +
                      "12. Translate `OUTER APPLY` to `LEFT JOIN LATERAL`, as `OUTER APPLY` is not directly supported in PostgreSQL.\n" +
                      "13. Translate MSSQL data types such as `DATETIME` to `TIMESTAMP`, `UNIQUEIDENTIFIER` to `UUID`, `MONEY` to `NUMERIC(19,4)`, and `BIT` to `BOOLEAN`.\n" +
                      "14. Convert `IDENTITY` columns to `SERIAL` or `BIGSERIAL` for auto-increment fields.\n" +
                      "15. Use `INSERT ON CONFLICT` as an alternative to `MERGE`, as PostgreSQL does not directly support `MERGE`.\n" +
                      "16. Adapt any table locking strategies for PostgreSQL using the `LOCK` statement.\n" +
                      "17. Ensure index and constraint compatibility with PostgreSQL’s indexing methods.\n" +
                      "18. Translate `WHILE` loops from MSSQL to PostgreSQL `LOOP`, `FOR`, or `WHILE` constructs.\n" +
                      "19. Add `DROP TABLE IF EXISTS <table_name>;` at the end to clean up temporary tables."
                    : $"Continue converting the MSSQL stored procedure to PostgreSQL (Part {i + 1} of {chunks.Count}):\n{chunk}";

                if (i == chunks.Count - 1)
                {
                    content +=
                        "\nPlease ensure the code ends with `$$ LANGUAGE plpgsql;` without any additional comments.";
                }

                var requestBody = new
                {
                    model = "gpt-3.5-turbo",
                    messages = new[]
                    {
                        new { role = "system", content = "You are a helpful assistant that converts SQL code." },
                        new
                        {
                            role = "user",
                            content = content
                        }
                    },
                    max_tokens = 1500,
                    temperature = 0.2,
                };


                using var request =
                    new HttpRequestMessage(HttpMethod.Post, "https://api.openai.com/v1/chat/completions")
                    {
                        Content = new StringContent(JsonConvert.SerializeObject(requestBody), Encoding.UTF8,
                            "application/json")
                    };

                request.Headers.Add("Authorization", $"Bearer {_apiKey}");

                var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync();
                var result = JsonConvert.DeserializeObject<GptResponse>(responseContent);

                var responseText = result?.Choices.FirstOrDefault()?.Message.Content ?? string.Empty;
                fullResult.Append(responseText);

                if (i == chunks.Count - 1)
                {
                    isComplete = IsProcedureContentComplete(responseText);

                    if (!isComplete)
                    {
                        fullResult.AppendLine("$$ LANGUAGE plpgsql;");
                        isComplete = true;
                    }
                }
                else
                {
                    isComplete = true;
                }
            }
        }

        return fullResult.ToString();
    }


    public async Task<string> ConvertProcedureToPgSql(string procedureText)
    {
        var fullResult = new StringBuilder();
        bool isComplete = false;
        int maxRetries = 5;
        int attempt = 0;

        while (!isComplete && attempt < maxRetries)
        {
            var requestBody = new
            {
                model = "gpt-3.5-turbo",
                messages = new[]
                {
                    new { role = "system", content = "You are a helpful assistant that converts SQL code." },
                    new
                    {
                        role = "user",
                        content = $"Convert the following MSSQL stored procedure to PostgreSQL:\n{procedureText}"
                    }
                },
                max_tokens = 1500,
                temperature = 0.2,
                //stop = new[] { "$$" }
            };

            using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.openai.com/v1/chat/completions")
            {
                Content = new StringContent(JsonConvert.SerializeObject(requestBody), Encoding.UTF8, "application/json")
            };

            request.Headers.Add("Authorization", $"Bearer {_apiKey}");

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadAsStringAsync();
            var result = JsonConvert.DeserializeObject<GptResponse>(responseContent);

            var responseText = result?.Choices.FirstOrDefault()?.Message.Content ?? string.Empty;
            fullResult.Append(responseText);
        
            // Используем IsContentComplete для проверки завершенности
            isComplete = IsProcedureContentComplete(responseText);

            if (!isComplete)
            {
                procedureText += "Please continue from where the previous response left off.";
                attempt++;
                await Task.Delay(1000);
            }
        }

        return fullResult.ToString();
    }

    public async Task SaveProcedureToFile(string procedureName, string content)
    {
        var filePath = Path.Combine("ConvertedProcedures", $"{procedureName}.sql");
        Directory.CreateDirectory("ConvertedProcedures");
        await File.WriteAllTextAsync(filePath, content);
    }
    
    public bool CheckFileExists(string procedureName)
    {
        var filePath = GetFilePath(procedureName);
        return File.Exists(filePath);
    }

    public async Task<bool> IsFileComplete(string procedureName)
    {
        var filePath = GetFilePath(procedureName);
        var fileContent = await File.ReadAllTextAsync(filePath);

        return IsProcedureContentComplete(fileContent);
    }
    
    public bool IsProcedureContentComplete(string content)
    {
        var trimmedContent = content.Trim();

        int dollarSignIndex = trimmedContent.IndexOf("$$");
        int languageIndex = trimmedContent.IndexOf("LANGUAGE plpgsql;", dollarSignIndex + 2);

        return dollarSignIndex != -1 && languageIndex != -1 && languageIndex > dollarSignIndex;
    }


    public string GetFilePath(string procedureName)
    {
        return Path.Combine("ConvertedProcedures", $"{procedureName}.sql");
    }
    
    public void ArchiveGeneratedScripts(string folderPath)
    {
        if (!Directory.Exists(folderPath))
        {
            Console.WriteLine("Папка не найдена.");
            return;
        }

        var files = Directory.GetFiles(folderPath);
        if (files.Length == 0)
        {
            Console.WriteLine("Нет файлов для архивирования.");
            return;
        }

        var folderName = Path.GetFileName(folderPath);
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var zipFileName = $"{folderName}_{timestamp}.zip";
        var zipFilePath = Path.Combine(folderPath, zipFileName);

        using (var zipArchive = ZipFile.Open(zipFilePath, ZipArchiveMode.Create))
        {
            foreach (var file in files)
            {
                zipArchive.CreateEntryFromFile(file, Path.GetFileName(file));
            }
        }

        Console.WriteLine($"Архив создан: {zipFilePath}");
    }
}