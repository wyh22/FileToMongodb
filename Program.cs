
using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FileToMongodb
{
    internal class Program
    {      

        private static async Task Main(string[] args)
        {
            string SourceAdress;
            string Mongodb;

            if (ConfigurationManager.AppSettings["SourceAdress"] != null)
            {
                SourceAdress = ConfigurationManager.AppSettings["SourceAdress"];
            }
            else
            {
                SourceAdress = @"C:\Users\D-06\Desktop\test";
            }
            if (ConfigurationManager.AppSettings["MongodbAdress"] != null)
            {
                Mongodb = ConfigurationManager.AppSettings["MongodbAdress"];
            }
            else
            {
                Mongodb = "mongodb://localhost:27017";
            }
            var time = new Stopwatch();
            time.Start();
            await TransFile.TransFilesAsync(SourceAdress, Mongodb);
            time.Stop();
            Console.WriteLine("插入失败次数：{0}",TransFile.failNumber);
            Console.WriteLine("插入文件总用时：{0}秒", time.ElapsedMilliseconds/1000.000);
            Console.ReadKey();
        }

    }
}

