using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace RealPosi
{
    class Logger
    {
        static public void WriteLog_text(string text)
        {
            string strFilePath = "log.txt";
            //FileStream fs = new FileStream(strFilePath, FileMode.Append);

            FileStream fs = new FileStream(strFilePath, FileMode.Append, FileAccess.Write, FileShare.Write);
            StreamWriter streamWriter = new StreamWriter(fs);
            streamWriter.BaseStream.Seek(0, SeekOrigin.End);
            streamWriter.WriteLine(DateTime.Now.ToString("yyyy'-'MM'-'dd' 'HH':'mm':'ss.ff") + "：" + text);
            streamWriter.Flush();
            streamWriter.Close();
            Console.WriteLine(DateTime.Now.ToString("yyyy'-'MM'-'dd' 'HH':'mm':'ss.fff") + "：" + text);
        }
    }
}
