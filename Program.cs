using AIGPTMigrateProcedureMSToPG.Models;
using AIGPTMigrateProcedureMSToPG.Services;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: false, reloadOnChange: true)
    .AddJsonFile("appsettings.Secrets.json", optional: true, reloadOnChange: true);

builder.Services.Configure<OpenAIOptions>(builder.Configuration.GetSection("OpenAI"));

builder.Services.AddHttpClient<ProcedureService>((serviceProvider, client) =>
{
    var options = serviceProvider.GetRequiredService<IOptions<OpenAIOptions>>().Value;
    client.BaseAddress = new Uri(options.BaseUrl);
});

builder.Services.AddSingleton(serviceProvider =>
{
    var configuration = serviceProvider.GetRequiredService<IConfiguration>();
    return configuration.GetConnectionString("MSSQL");
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "AI GPT Migrate API", Version = "v1" });
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "AI GPT Migrate API v1");
        c.RoutePrefix = string.Empty;
    });
}

var configuration = builder.Configuration;

var apiKey = configuration["OpenAI:ApiKey"];

app.MapGet("/convert-procedures", async (ProcedureService procedureService) =>
{
    string folderPath = "ConvertedProcedures";
    procedureService.ArchiveGeneratedScripts(folderPath);
    
    var storedProcedures = await procedureService.GetStoredProceduresFromMssql();
    var results = new List<string>();

    foreach (var (procedureName, procedureDefinition) in storedProcedures)
    {
        Console.WriteLine($"Started convert \"{procedureName}\"...");
        if (procedureService.CheckFileExists(procedureName))
        {
            bool isComplete = await procedureService.IsFileComplete(procedureName);
        
            if (isComplete)
            {
                var completedContent = await File.ReadAllTextAsync(procedureService.GetFilePath(procedureName));
                results.Add(completedContent);
                Console.WriteLine($"Skipped procedure \"{procedureName}\"...");
                continue;
            }
            else
            {
                File.Delete(procedureService.GetFilePath(procedureName));
                Console.WriteLine($"Deleted file procedure \"{procedureName}\"... Try again...");
            }
        }
        
        var convertedProcedure = await procedureService.ConvertLongProcedureToPgSql(procedureDefinition);
        results.Add(convertedProcedure);
        
        await procedureService.SaveProcedureToFile(procedureName, convertedProcedure);
        Console.WriteLine($"Procedure is completed \"{procedureName}\"...");
    }

    return Results.Ok(results);
});

app.Run();