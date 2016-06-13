using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.IO;

namespace System.Collections.Generic
{
    public static class ICollectionExtensions
    {
        public static TCollection With<TCollection, T>(this TCollection collection, T value)
            where TCollection : ICollection<T>
        {
            collection.Add(value);
            return collection;
        }
    }
}

namespace ViewSource
{
    public class App
    {
        public static void Main(string[] args)
        {
            var parse = new ArgumentParser(args);
            var context = new ConversionContext()
            {
                Database = parse.Arg<string>("d"),
                //OutputStream = new SplitStream(parse.HasArg("o") ? new FileInfo(parse.Arg<string>("o")).OpenWrite() : Console.OpenStandardOutput()),
                Password = parse.Arg<string>("p"),
                Server = parse.Arg<string>("s"),
                Tables = parse.HasArg("t") ? parse.Arg<string>("t").Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries) : null,
                Username = parse.Arg<string>("u"),
                ElasticDataSource = parse.Arg<string>("e"),
                Credential = parse.Arg<string>("c"),
                ScriptElasticDataSource = parse.HasArg("ds")
            };

            var streams = new List<Stream>();
            if (parse.HasArg("o"))
            {
                streams.Add(new FileInfo(parse.Arg<string>("o")).OpenWrite());
            }

            if (parse.HasArg("v") || !streams.Any())
            {
                streams.Add(Console.OpenStandardOutput());
            }

            context.OutputStream = new MulticastStream(streams, true);

            Script(context);

            context.OutputStream.Dispose();
        }
        private static string Q(StreamWriter writer, string v)
        {
            writer.WriteLine(v);
            writer.WriteLine(string.Empty);
            writer.Flush();
            return v;
        }
        private static string Replace(string value, string datasource)
        {
            return value.Replace("CREATE TABLE", "CREATE EXTERNAL TABLE")
                    .Replace("ON [PRIMARY]", $"\r\nWITH\r\n(DATA_SOURCE={datasource})");
        }

        private static void Script(ConversionContext context)
        {
            using (var writer = new StreamWriter(context.OutputStream))
            {
                if (context.ScriptElasticDataSource)
                {
                    Q(writer, $"CREATE EXTERNAL DATA SOURCE {context.ElasticDataSource} WITH (\r\nTYPE = RDBMS,\r\nLOCATION='{context.Server}',\r\nDATABASE_NAME='{context.Database}',\r\nCREDENTIAL={context.Credential})\r\nGO");
                }
                var list = new Server(new ServerConnection(context.Server, context.Username, context.Password))
                    .Databases[context.Database]
                    .Tables.OfType<Table>()
                    .Where(x => context.Tables == null || !context.Tables.Any()
                        ? true
                        : context.Tables
                            .Any(y => y.Equals(x.Name, StringComparison.OrdinalIgnoreCase)))
                    .SelectMany(x =>
                        x.Script(new ScriptingOptions
                        {
                            SchemaQualify = false,
                            DriAll = false,

                        })
                            .Cast<string>()
                            .Where(y => y.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                            .Select(s => Q(writer, Replace(s + "\r\n" + "GO", context.ElasticDataSource))))
                    .ToList();
            }

        }
    }

    public class ArgumentParser
    {
        private static readonly string[] _empty = new string[0];
        private static readonly string[] Leading = { "-", "/" };
        private readonly IEnumerable<string> _arguments;
        private Lazy<IEnumerable<KeyValuePair<string, IEnumerable<string>>>> _parsed;

        public ArgumentParser() : this(CommandLineArgs)
        {

        }

        public ArgumentParser(IEnumerable<string> arguments)
        {
            _arguments = arguments;
            _parsed = new Lazy<IEnumerable<KeyValuePair<string, IEnumerable<string>>>>(Parse);
        }

        private delegate bool TryParse<T>(string value, out T result);

        public static string[] CommandLineArgs => Environment.GetCommandLineArgs().Skip(1).ToArray();

        /// <summary>
        /// Gets the parsed arguments
        /// </summary>
        public IEnumerable<KeyValuePair<string, IEnumerable<string>>> Parsed => _parsed.Value;

        /// <summary>
        /// Gets or sets the separator used to parse arguments that have values. If setting a custom separator, this should be set before
        /// accessing other members.
        /// </summary>
        public string Separator { get; set; } = ":";
        public T Arg<T>(string name) => As<T>(name).FirstOrDefault();

        public IEnumerable<T> As<T>(string name)
        {
            if (!HasArg(name))
            {
                yield break;
            }

            foreach (var value in Parsed.First(x => x.Key.Equals(name, StringComparison.OrdinalIgnoreCase)).Value)
            {
                yield return Convert<T>(value);
            }
        }

        public bool HasArg(string name) => Parsed.Any(x => x.Key.Equals(name, StringComparison.OrdinalIgnoreCase));
        private bool ContainsPath(string arg) => arg.Contains(":\\");

        private T Convert<T>(string value)
        {
            var result = default(T);

            var tryParse = (TryParse<T>)typeof(T).GetMethod("TryParse",
                new[] { typeof(string), typeof(T).MakeByRefType() })?.CreateDelegate(typeof(TryParse<T>));

            if (tryParse != null)
            {
                tryParse(value, out result);
            }
            else if (typeof(T) == typeof(string))
            {
                result = (T)((object)value);
            }

            return result;
        }
        private string GetName(string arg)
        {
            arg = ((bool)(arg?.Contains(Separator)) ? arg.Substring(0, arg.IndexOf(Separator)) : arg);
            arg = Leading.Aggregate(arg, (a, x) => a.Replace(x, ""), x => x);
            return arg;
        }

        private string[] GetValues(string arg) =>
                    ContainsPath(arg) && Separator == ":"
                ? new[] { arg.Substring(arg.IndexOf(Separator) + 1) }
                : arg.Split(new[] { Separator }, StringSplitOptions.RemoveEmptyEntries).Skip(1).ToArray();
        private IEnumerable<KeyValuePair<string, IEnumerable<string>>> Parse() =>
             _arguments.Select(x => new KeyValuePair<string, IEnumerable<string>>(GetName(x), GetValues(x).AsEnumerable()));
    }

    public class ConversionContext
    {
        public object Credential { get; set; }
        public string Database { get; set; }
        public string ElasticDataSource { get; set; }
        public Stream OutputStream { get; set; }
        public string Password { get; set; }
        public bool ScriptElasticDataSource { get; set; }
        public string Server { get; set; }
        public IEnumerable<string> Tables { get; set; }
        public string Username { get; set; }
    }

    class MulticastStream : Stream
    {
        private bool _ownsStreams;
        public MulticastStream(IEnumerable<Stream> streams, bool ownsStreams = false)
        {
            Sources = streams;
            _ownsStreams = ownsStreams;
        }

        public override bool CanRead => Sources.Any(x => x.CanRead);

        public override bool CanSeek => Sources.All(x => x.CanSeek);

        public override bool CanWrite => true;

        public override long Length => (long)Sources.Average(x => x.Length);

        public override long Position
        {
            get
            {
                return Sources.First().Position;
            }
            set
            {
                All(x => x.Position = value);
            }
        }

        public IEnumerable<Stream> Sources { get; set; }

        public override void Flush()
        {
            All(x => x.Flush());
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var tmp = Sources.FirstOrDefault(x => x.CanRead)?.Read(buffer, offset, count);
            return tmp.HasValue ? tmp.Value : 0;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return All(x => x.Seek(offset, origin)).First();
        }

        public override void SetLength(long value)
        {
            All(x => x.SetLength(value));
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            All(x => x.Write(buffer, offset, count));
        }

        protected override void Dispose(bool disposing)
        {
            if (_ownsStreams)
            {
                All(x => x.Dispose());
            }
            base.Dispose(disposing);
        }
        private void All(Action<Stream> action)
        {
            foreach (var stream in Sources)
            {
                action(stream);
            }
        }
        private IEnumerable<T> All<T>(Func<Stream, T> action)
        {
            foreach (var stream in Sources)
            {
                yield return action(stream);
            }
        }
    }
}
