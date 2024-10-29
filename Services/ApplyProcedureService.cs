using System.Data.SqlClient;
using System.Text;
using AIGPTMigrateProcedureMSToPG.Models;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Npgsql;

namespace AIGPTMigrateProcedureMSToPG.Services;

public class ApplyProcedureService
{
    private readonly string _connectionString;
    private readonly string _apiKey;
    private readonly HttpClient _httpClient;
    
    public ApplyProcedureService(HttpClient httpClient, IOptions<OpenAIOptions> openAiOptions, IOptions<ConnectionStrings> connectionOptions)
    {
        _httpClient = httpClient;
        _httpClient.BaseAddress = new Uri(openAiOptions.Value.BaseUrl);
        _apiKey = openAiOptions.Value.ApiKey;
        _connectionString = connectionOptions.Value.Postgres;
    }

    public async Task ApplyProceduresAsync(string baseFolderPath = "base", string schemaDb = "master_dbo")
    {
        var acceptPath = Path.Combine(baseFolderPath, "accept");
        var declinePath = Path.Combine(baseFolderPath, "decline");

        Directory.CreateDirectory(acceptPath);
        Directory.CreateDirectory(declinePath);

        foreach (var filePath in Directory.GetFiles(baseFolderPath, "*.sql").OrderBy(Path.GetFileName))
        {
            // Чтение содержимого и удаление лишних частей
            var procedureText = await File.ReadAllTextAsync(filePath);
            if (procedureText.StartsWith("```"))
            {
                procedureText = procedureText.Substring(3);
            }

            if (procedureText.EndsWith("```  "))
            {
                procedureText = procedureText.Substring(0, procedureText.Length - 5);
            }else                     
            if (procedureText.EndsWith("```"))
            {
                procedureText = procedureText.Substring(0, procedureText.Length - 3);
            }

            if (procedureText.StartsWith("sql", StringComparison.OrdinalIgnoreCase))
            {
                procedureText = procedureText.Substring(3).Trim();
            }

            var fileName = Path.GetFileName(filePath);

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var setSchemaCommand = new NpgsqlCommand($"SET search_path TO {schemaDb};", connection);
                await setSchemaCommand.ExecuteNonQueryAsync();

                using var command = new NpgsqlCommand(procedureText, connection);
                await command.ExecuteNonQueryAsync();

                var destinationPath = Path.Combine(acceptPath, fileName);
                File.Move(filePath, destinationPath);
                Console.WriteLine($"{fileName} выполнена успешно и перемещена в 'accept'");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{fileName} не выполнена: {ex.Message}. Попытка обработки через GPT...");
                string acceptWithGptPath = "ConvertedProcedures\\acceptWithGptPath";
                string lastError = ex.Message;  // Переменная для хранения последней ошибки
                string gptCorrectedProcedureText = procedureText;

                for (int attempt = 1; attempt <= 3; attempt++)
                {
                    try
                    {
                        // Отправка запроса к GPT с последней ошибкой и текстом процедуры
                        gptCorrectedProcedureText = await SendRequestToGptAsync(gptCorrectedProcedureText, lastError);
                        if (gptCorrectedProcedureText.StartsWith("```"))
                        {
                            gptCorrectedProcedureText = gptCorrectedProcedureText.Substring(3);
                        }

                        if (gptCorrectedProcedureText.EndsWith("```  "))
                        {
                            gptCorrectedProcedureText = gptCorrectedProcedureText.Substring(0, gptCorrectedProcedureText.Length - 5);
                        }else                     
                        if (gptCorrectedProcedureText.EndsWith("```"))
                        {
                            gptCorrectedProcedureText = gptCorrectedProcedureText.Substring(0, gptCorrectedProcedureText.Length - 3);
                        }

                        if (gptCorrectedProcedureText.StartsWith("sql", StringComparison.OrdinalIgnoreCase))
                        {
                            gptCorrectedProcedureText = gptCorrectedProcedureText.Substring(3).Trim();
                        }
                        

                        // Подключение к базе данных и выполнение исправленного SQL-запроса
                        using var connection = new NpgsqlConnection(_connectionString);
                        await connection.OpenAsync();

                        using var correctedCommand = new NpgsqlCommand(gptCorrectedProcedureText, connection);
                        await correctedCommand.ExecuteNonQueryAsync();

                        // Замена содержимого файла на исправленную процедуру
                        await File.WriteAllTextAsync(filePath, gptCorrectedProcedureText);
        
                        var destinationPath = Path.Combine(acceptWithGptPath, fileName);
                        File.Move(filePath, destinationPath);
                        Console.WriteLine($"{fileName} успешно выполнена после обработки GPT. Содержимое файла обновлено и перемещено в 'acceptwithgpt'");
                        break; // Выход из цикла при успешном выполнении
                    }
                    catch (Exception retryEx)
                    {
                        lastError = retryEx.Message;  // Обновление сообщения об ошибке для следующей попытки
                        Console.WriteLine($"{fileName} не выполнена в попытке {attempt} через GPT: {retryEx.Message}");

                        if (attempt == 3)
                        {
                            // После трех попыток перемещаем в 'decline'
                            var destinationPath = Path.Combine(declinePath, fileName);
                            File.Move(filePath, destinationPath);
                            Console.WriteLine($"{fileName} не выполнена после трех попыток обработки GPT. Перемещена в 'decline'");
                        }
                    }
                }
            }
        }
    }

    public async Task<string> SendRequestToGptAsync(string procedureText, string errorMessage)
    {

        // Создание запроса с настройками
        var requestBody = new
        {
            model = "gpt-4o",
            messages = new[]
            {
                new
                {
                    role = "system",
                    content =
                        "You are an assistant specializing in converting SQL queries to be compatible with PostgreSQL."
                },
                new
                {
                    role = "user",
                    content =
                        $"I encountered an error while executing an SQL query in PostgreSQL. The error message is: '{errorMessage}'. " +
                        $"Please correct the following SQL code so that it works in PostgreSQL:\n\n{procedureText}\n\n" +
                        "Ensure that the result is compatible with PostgreSQL, and replace any Microsoft SQL-specific " +
                        "operators and functions with their PostgreSQL equivalents if necessary. " +
                        "Return only the corrected SQL code without any additional comments or extraneous symbols."
                }
            },
            temperature = 0.2
        };

        // Создание HttpRequestMessage для отправки запроса
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.openai.com/v1/chat/completions")
        {
            Content = new StringContent(JsonConvert.SerializeObject(requestBody), Encoding.UTF8, "application/json")
        };

        // Добавление заголовка авторизации
        request.Headers.Add("Authorization", $"Bearer {_apiKey}");

        // Отправка запроса и получение ответа
        var response = await _httpClient.SendAsync(request);
        response.EnsureSuccessStatusCode();

        string responseContent = await response.Content.ReadAsStringAsync();

        // Декодирование ответа от GPT
        var gptResponse = JsonConvert.DeserializeObject<dynamic>(responseContent);
        string correctedSql = gptResponse.choices[0].message.content;

        return correctedSql;
    }
}