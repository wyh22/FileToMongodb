using System;
using System.IO;
using System.Text;

namespace FileToMongodb
{
    internal class ErrorLog
    {
        public static void ErrorLogTxt(Exception ex)
        {
            string FilePath = AppDomain.CurrentDomain.BaseDirectory + "/ErrorLog.txt";

            StringBuilder msg = new StringBuilder();
            msg.Append("*************************************** \r\n");
            msg.AppendFormat(" 异常信息： {0} \r\n", ex.Message);
            msg.AppendFormat(" 异常发生时间： {0} \r\n", DateTime.Now);
            msg.AppendFormat(" 异常类型： {0} \r\n", ex.HResult);
            msg.AppendFormat(" 导致当前异常的 Exception 实例： {0} \r\n", ex.InnerException);
            msg.Append("***************************************");

            // msg.AppendFormat(" 导致异常的应用程序或对象的名称： {0} \r\n", ex.Source);
            //msg.AppendFormat(" 引发异常的方法： {0} \r\n", ex.TargetSite);
            //msg.AppendFormat(" 异常堆栈信息： {0} \r\n", ex.StackTrace);

            if (File.Exists(FilePath))//如果文件存在
            {
                using (StreamWriter tw = File.AppendText(FilePath))
                {
                    tw.WriteLine(msg.ToString());
                }
            }
            else
            {
                TextWriter tw = new StreamWriter(FilePath);
                tw.WriteLine(msg.ToString());
                tw.Flush();
                tw.Close();
            }
        }
    }
}