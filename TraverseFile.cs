using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace FileToMongodb
{
    class TraverseFile
    {
        //暂时写死在19级,存储分类文件的二重列表
        static List<List<BsonDocument>> collectionList = ListFactory(19);
        public static int failNumber = 0;
        //struct FileBelong
        //{
        //    string databaseName;
        //    string collectionName;
        //};
        public static async Task TraverseFilesAsync(string directoryPath, string toMongodb)
        {

            var filePaths = new ConcurrentQueue<string>(Directory.GetFiles(directoryPath, "*", SearchOption.AllDirectories));

            //建立连接
            var client = new MongoClient(toMongodb);

            while (filePaths.TryDequeue(out var filePath))
            {
                if (!filePath.ToLower().EndsWith(".jpg"))
                {
                    continue;
                }
                fileClassifyTolabel(filePath);    //文件插入对应collection列表，label序号减一对应collection列表序号           
            }
            for (int i = 0; i < collectionList.Count; i++)
            {
                if (collectionList[i].Count == 0) continue;


                //这里输入了label集合名
                await InFilesAsync(client, "Level19", string.Format("Titles{0:D2}", i + 1), collectionList[i]);
            }

            Console.WriteLine();
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

        static int LXYToTableNumber(int level, int x, int y)
        {
            int result = 1;
            if (level > 15)
            {
                int num = (x - ((int)(Math.Pow(2.0, (double)level) / 4.0))) / ((int)Math.Pow(2.0, 14.0));
                int num2 = y / (int)(Math.Pow(2, 15));

                int num3 = num * (int)Math.Pow(2, level - 15) + num2 + 1;

                result = num3;
            }
            return result;
        }
        //根据级别和坐标返回表名
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

        //集合列表文件批量传输
        static async Task InFilesAsync(MongoClient client, string databaseName, string collectionName, List<BsonDocument> documents)
        {
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            try { await collection.InsertManyAsync(documents); }
            catch (Exception ex)
            {
                ErrorLog.ErrorLogTxt(ex);
                Console.WriteLine("插入异常");
                ++failNumber;
            }

        }


        //把文件插入对应的集合
        static void fileClassifyTolabel(string filePath)
        {
            //从文件名，获取转换信息
            int[] Lxy = LXYGet(filePath);               //从文件目录读取lxy
            int labelNumber = LXYToTableNumber(Lxy[0], Lxy[1], Lxy[2]);   //集合序号


            //根据文件路径，组合单个document
            using (FileStream stream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var document = new BsonDocument
                    {
                            { "_id", IdGet(filePath) },
                            { "ByteImg", new BsonBinaryData(reader.ReadBytes((int)stream.Length)) }
                    };
                    collectionList[labelNumber - 1].Add(document);
                }
            }
        }

        //输入那一个数据库的label总数,list有序，生成二重列表
        static List<List<BsonDocument>> ListFactory(int level)
        {
            int number = 1;
            if (level > 15) {
                number = (int)Math.Pow(2, level - 15)* (int)Math.Pow(2, level - 15);
            }
            List<List<BsonDocument>> collectionsL = new List<List<BsonDocument>>();
            for (int i = 1; i <= number; i++)
            {
                List<BsonDocument> term = new List<BsonDocument>();
                collectionsL.Add(term);
            }
            return collectionsL;
        }

    }
}

