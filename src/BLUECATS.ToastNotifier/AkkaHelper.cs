using System;
using System.IO;
using System.Reflection;
using Akka.Configuration;
using DevExpress.Xpo;

namespace BLUECATS.ToastNotifier
{

    public static class AkkaHelper
    {
        public static Config ReadConfigurationFromHoconFile(Assembly assembly, string hoconFileExtension)
        {
            var assemblyFilePath = new Uri(assembly.GetName().CodeBase).LocalPath;
            var assemblyDirectoryPath = Path.GetDirectoryName(assemblyFilePath);
            var hoconFileName = Path.GetFileNameWithoutExtension(assemblyFilePath);
            var hoconFilePath = $@"{assemblyDirectoryPath}{Path.DirectorySeparatorChar}{hoconFileName}.{hoconFileExtension}";
            return ConfigurationFactory.ParseString(File.ReadAllText(hoconFilePath)); // Akka.net 제공
        }

    }


}