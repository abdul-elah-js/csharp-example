using System;
using Newtonsoft.Json;
using System.Data;
using System.Net.Http;
using System.Text;
using System.Linq;
using MySql.Data.MySqlClient; // MySql Adapter

// Install System.Data.DataSetExtensions to Access AsEnumerable()

namespace integrationExampleUsingCsharp
{
  class Program
  {
    private const string RequestUri = "http://139.59.151.199/api/sync_store_products_by_key"; // Endpoint URL
    private const string Token = "JWT Token"; // JWT Token
    private const string query = "select * from users"; // Query String
    static void Main(string[] args)
        {
            string connStr = "server=localhost;database=dotnet;uid=root;pwd=root"; // Connection String
            using (MySqlConnection conn = new MySqlConnection(connStr))
            {
                try
                {
                    Console.WriteLine("Connecting To MySql ...");
                    using (MySqlDataAdapter da = new MySqlDataAdapter()) // Generate The MySql Adapter 
                    {
                        using (DataTable dt = new DataTable())
                        {
                            using (MySqlCommand sqlCommand = conn.CreateCommand()) // Preparing MySql Command
                            {
                                sqlCommand.CommandType = CommandType.Text;
                                sqlCommand.CommandText = query;
                                da.SelectCommand = sqlCommand; // Execute MySql Command
                                da.Fill(dt); // Dump Result into DataTable
                                sqlCommand.Dispose();
                                da.Dispose();
                                Console.WriteLine("Preparing Batches");
                                var tr = dt.Rows.Count; // Get Total Rows Count
                                tr = (int)Math.Ceiling((Double)tr / 1000); // Divide total by the batches
                                var skipCount = 0;
                                for (int i = 0; i < tr; i++) // Loop through the total Table rows
                                {
                                    Console.WriteLine("Sending " + (i + 1) + " Batch");
                                    var dataBatch = dt.AsEnumerable().Skip(skipCount).Take(1000); // Take 
                                    using (DataTable copyTableData = dataBatch.CopyToDataTable<DataRow>()) // Copy Batches to New DataTable
                                    {
                                        var jsonResult = JsonConvert.SerializeObject(copyTableData); // Serialzie Copied Data To JSON Object
                                        sendRequest(jsonResult);
                                        skipCount += 1000;
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                  Console.WriteLine(e.Message);
                }
            }
            Console.WriteLine("");
            Console.WriteLine("Done ...");
        }

    static void sendRequest(String data)
    {
      try
      {
        using (HttpClient client = new HttpClient())
          {
              client.DefaultRequestHeaders.Add("Authorization", Token); // Add Authorization Header

              using (HttpContent content = new StringContent(data, Encoding.UTF8, "application/json")) // Prepare JSON Payload To Send
              {
                  using (HttpResponseMessage response = client.PostAsync(RequestUri, content).Result) // Send Http Request And Store Result In (response)
                  {
                      Console.WriteLine(response.Content.ReadAsStringAsync().Result); // Print Out Response
                      Console.WriteLine("Batch Sent");
                  }
              }
          }
      }
      catch (Exception e)
      {
        Console.WriteLine(e.Message);
      }
    }
  }
}
