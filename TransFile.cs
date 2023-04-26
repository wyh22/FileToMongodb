using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace FileToMongodb
{
    class TransFile
    {
        //在本地模拟数据库的结构，用来将文件分类
        static Dictionary<string, Dictionary<string, List<BsonDocument>>> localDb = new Dictionary<string, Dictionary<string, List<BsonDocument>>>();

        //存储一批已经插入数据库的路径，辅助文件删除
        static Dictionary<string, List<string>> filePathList = new Dictionary<string, List<string>>();

        public static int failNumber = 0;

        public static async Task TransFilesAsync(string directoryPath, string toMongodb)
        {
            Console.WriteLine("文件名统计开始");
            var filePaths = new ConcurrentQueue<string>(Directory.GetFiles(directoryPath, "*", SearchOption.AllDirectories));
            Console.WriteLine("文件名统计结束");
           
            try
            {   //建立连接
                var client = new MongoClient(toMongodb);


                //扫描目录文件
                while (filePaths.TryDequeue(out var filePath))
                {
                    if (!filePath.ToLower().EndsWith(".jpg"))
                    {
                        continue;
                    }
                    int[] lxy = LXYGet(filePath);
                    string dbName = LevelToDBName(lxy[0]);
                    string labelName = LXYToTableName(lxy[0], lxy[1], lxy[2]);

                    //没有该数据库，直接加入文件对应数据库和集合
                    if (!localDb.ContainsKey(dbName))
                    {

                        localDb.Add(dbName, new Dictionary<string, List<BsonDocument>>() { [labelName] = new List<BsonDocument>() });
                        if (!filePathList.ContainsKey(dbName + labelName))
                        {
                            filePathList.Add(dbName + labelName, new List<string>());
                        }

                    }
                    //有数据库没集合，加入对应集合
                    else if (!localDb[dbName].ContainsKey(labelName))
                    {
                        localDb[dbName].Add(labelName, new List<BsonDocument>());
                        //filePathList.Add(dbName + labelName, new List<string>());
                    }
                    using (FileStream stream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                    {
                        using (var reader = new BinaryReader(stream))
                        {
                            var document = new BsonDocument
                    {
                            { "_id", IdGet(filePath) },
                            { "ByteImg", new BsonBinaryData(reader.ReadBytes((int)stream.Length)) }
                    };

                            //插入本地伪数据库
                            localDb[dbName][labelName].Add(document);

                            //对应文件目录插入列表
                            filePathList[dbName + labelName].Add(filePath);

                            //一次传一列
                            if (localDb[dbName][labelName].Count > 1000)  //假设一张瓦片10kb，100占用内存，约1mb
                            {
                                //列中数据大于100插入数据库
                                await SaveAsync(localDb[dbName][labelName],filePathList[dbName+labelName], client.GetDatabase(dbName).GetCollection<BsonDocument>(labelName));
                            }
                        }

                    }

                }

                //余量文件插入
                foreach (var dbs in localDb) {
                    foreach (var title in dbs.Value)
                    {
                        var db = client.GetDatabase(dbs.Key);
                        var collection = db.GetCollection<BsonDocument>(title.Key);
                       await SaveAsync(localDb[dbs.Key][title.Key], filePathList[dbs.Key + title.Key], client.GetDatabase(dbs.Key).GetCollection<BsonDocument>(title.Key));

                    }
                }

            }
            catch (Exception ex) { ErrorLog.ErrorLogTxt(ex); }



            Console.WriteLine();
        }

        //插入一个集合中数据
        public static async Task SaveAsync(List<BsonDocument> list,List<string> paths,IMongoCollection<BsonDocument> collection)
        {
            if (list.Count > 0 && collection != null)
            {
                try
                {
                    await collection.InsertManyAsync(list);
                    list.Clear();
                    foreach (var file in paths)
                    {
                        try { File.Delete(file); }
                        catch (Exception ex) { ErrorLog.ErrorLogTxt(ex); }
                    }
                    paths.Clear();
                }
                catch (Exception ex)
                {
                    ErrorLog.ErrorLogTxt(ex);
                    Console.WriteLine("插入异常");
                    ++failNumber;
                }
            }
        }

        static string LevelToDBName(int level)
        {
            if (level >= 1 && level <= 14)
            {
                return "Level1-Level14";
            }
            else if (level > 14)
            {
                return string.Format("Level{0}", level);
            }
            return "";
        }

        //0==level,1==x,2==y;转化后
        static int[] LXYGet(string filePath)
        {
            int[] array = new int[3];

            //乔峰数据的标号Imgname1
            var Imgname1 = Path.GetFullPath(filePath);
            var numberL = Imgname1.Split('\\');
            Array.Reverse(numberL);
            int l1, x1, y1;
            l1 = int.Parse(numberL[2]);
            x1 = int.Parse(numberL[1]);
            y1 = int.Parse(Path.GetFileNameWithoutExtension(filePath));

            array[0] = l1 + 1;
            array[2] = x1;
            array[1] = (int)(Math.Pow(2, l1) * (3.0f / 2) - y1 - 1);
            return array;
        }


        static string LXYToTableName(int level, int x, int y)
        {
            string ret = "Titles";

            if (level > 15)
            {
                int num = (x - ((int)(Math.Pow(2.0, (double)level) / 4.0))) / ((int)Math.Pow(2.0, 14.0));
                int num2 = y / (int)(Math.Pow(2, 15));

                int num3 = num * (int)Math.Pow(2, level - 15) + num2 + 1;

                ret = string.Format("Titles{0:D2}", num3);
            }
            return ret;
        }

        static string IdGet(string filePath)
        {
            int[] array = new int[3];
            string result;
            //乔峰数据的标号Imgname1
            var Imgname1 = Path.GetFullPath(filePath);
            var numberL = Imgname1.Split('\\');
            Array.Reverse(numberL);

            int l1, x1, y1;
            l1 = int.Parse(numberL[2]);
            x1 = int.Parse(numberL[1]);
            y1 = int.Parse(Path.GetFileNameWithoutExtension(filePath));

            array[0] = l1 + 1;
            array[2] = x1;
            array[1] = (int)(Math.Pow(2, l1) * (3.0f / 2) - y1 - 1);
            //Console.WriteLine(Imgname1);

            //转换规则，待更改
            result = array[0].ToString() + "-" + array[1].ToString() + "-" + array[2].ToString();
            return result;
        }





    }
}

