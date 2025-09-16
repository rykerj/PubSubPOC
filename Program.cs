using System.ComponentModel.Design.Serialization;
using System.Security.Cryptography;
using System.Text.Unicode;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Eventbus.V1;
using Grpc.Core;
using Grpc.Net.Client;
using Newtonsoft.Json;
using SolTechnology.Avro;

namespace PubSubPOC
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            var channel = GrpcChannel.ForAddress("https://api.pubsub.salesforce.com:443");

            var client = new PubSub.PubSubClient(channel);

            var metadata = new Metadata()
            {
                { "accesstoken", "accesstoken"},
                { "instanceurl", "url" },
                { "tenantid", "tenantid" }
            };

            var newRequest = new FetchRequest
            { //"/events/My_Platform_Event__E"
                TopicName = "/event/Custom_Event__e",
                ReplayPreset = ReplayPreset.Earliest,
                NumRequested = 10
            };

            var what = await client.GetTopicAsync(new TopicRequest
            {
                TopicName = "/event/Custom_Event__e"
            }, metadata);
            Console.WriteLine($"Subscribing : {what.CanSubscribe}");

            var schema = await client.GetSchemaAsync(new SchemaRequest
            {
                SchemaId = what.SchemaId
            }, metadata);

            try
            {
                using var call = client.Subscribe(metadata);

                await call.RequestStream.WriteAsync(newRequest);

                await foreach (var response in call.ResponseStream.ReadAllAsync())
                {
                    if (response.Events.Count == 0)
                    {
                        Console.WriteLine("empty");
                        continue;
                    }

                    foreach (var responseEvent in response.Events)
                    {
                        var payloadBytes = responseEvent.Event.Payload.ToByteArray();
                        var genericRecord = Deserialize(payloadBytes, schema.SchemaJson);
                        Console.WriteLine(genericRecord);
                        Console.WriteLine(responseEvent.ReplayId.ToBase64());
                    }
                }
            }
            catch (RpcException ex)
            {
                Console.WriteLine($"grpc error: {ex.Status}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"unexpected error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static Dictionary<string, object> ConvertGenericObjectToDictionary(GenericRecord record)
        {
            var result = new Dictionary<string, object>();
            foreach (var field in record.Schema.Fields)
            {
                var fieldValue = record[field.Name];
                result[field.Name] = ConvertAvroValue(fieldValue);
            }

            return result;
        }

        private static object ConvertAvroValue(object fieldValue)
        {
            return fieldValue switch
            {
                GenericRecord nestedRecord => ConvertGenericObjectToDictionary(nestedRecord),
                IList<object> list => list.Select(ConvertAvroValue).ToList(),
                IDictionary<string, object> dict => dict.ToDictionary(kvp => kvp.Key, kvp => ConvertAvroValue(kvp.Value)),
                _ => fieldValue
            };
        }

        private static GenericRecord Deserialize(byte[] avroBytes, string schemaString)
        {
            var schema = Schema.Parse(schemaString);
            var reader = new GenericDatumReader<GenericRecord>(schema, schema);

            using var stream = new MemoryStream(avroBytes);
            var decoder = new BinaryDecoder(stream);
            var record = reader.Read(null, decoder);

            return record;
        }
    }
}
