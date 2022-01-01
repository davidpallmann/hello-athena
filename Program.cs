using Amazon.Athena;
using Amazon.Athena.Model;

public class Program
{
    static AmazonAthenaClient _client = null!;

    const int EmployeeIndex = 0, DateIndex = 1, AmountIndex = 2, ReasonIndex = 3;

    static async Task Main(string[] args)
    {
        Console.WriteLine("Issuing query to Athena");

        _client = new AmazonAthenaClient(Amazon.RegionEndpoint.USWest1);

        var queryRequest = new StartQueryExecutionRequest
        {
            QueryString = "SELECT employee,date,amount,reason from employee.expense",

            QueryExecutionContext = new QueryExecutionContext()
            {
                Database = "employee"
            },
            ResultConfiguration = new ResultConfiguration
            {
                OutputLocation = "s3://hello-cloud-athena/"
            }
        };

        var result = await _client.StartQueryExecutionAsync(queryRequest);

        // Retrieve results

        Console.WriteLine("Retrieving expenses");

        GetQueryResultsResponse results = null!;
        bool firstTime = true;
        string token = null!;
        Row index = null!;

        string queryExecutionId = result.QueryExecutionId;
        while (firstTime || token != null)
        {
            token = (firstTime ? null! : token);
            results = await GetQueryResult(queryExecutionId, token);

            int skipCount = 0;
            if (firstTime)
            {
                skipCount = 1;
                index = results.ResultSet.Rows[0];
            }

            Console.WriteLine($"Employee         Date           Amount Reason");
            foreach (var exp in results.ResultSet.Rows.Skip(skipCount))
            {
                Console.WriteLine($"{exp.Data[EmployeeIndex].VarCharValue,-16} {exp.Data[DateIndex].VarCharValue,-12} {exp.Data[AmountIndex].VarCharValue,8} {exp.Data[ReasonIndex].VarCharValue}"); 
            }

            token = results.NextToken;
            firstTime = false;
        }
    }

    /// <summary>
    /// GetQueryResult: Await and return Athena query result. If query is still executing, sleeps and retries up to 10 times.
    /// </summary>
    /// <param name="queryExecutionId">Athena query execution Id</param>
    /// <param name="token">continuation token (should be null the first time)</param>
    /// <returns>GetQueryResultsResponse object</returns>

    static async Task<GetQueryResultsResponse> GetQueryResult(string queryExecutionId, string token)
    {
        GetQueryResultsResponse results = null!;
        bool succeeded = false;
        int retries = 0;
        const int max_retries = 20;

        while (!succeeded)
        {
            try
            {
                results = await _client.GetQueryResultsAsync(new GetQueryResultsRequest()
                { QueryExecutionId = queryExecutionId, NextToken = token });
                succeeded = true;
            }
            catch (InvalidRequestException ex)
            {
                if (ex.Message.EndsWith("QUEUED") || ex.Message.EndsWith("RUNNING"))
                {
                    Thread.Sleep(1000 * 30);
                    retries++;
                    if (retries >= max_retries) throw ex;
                }
                else
                {
                    throw ex;
                }
            }
        }

        return results;
    }
}
