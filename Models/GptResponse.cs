namespace AIGPTMigrateProcedureMSToPG.Models;

public class GptResponse
{
    public List<Choice> Choices { get; set; }
    public Usage Usage { get; set; }
}