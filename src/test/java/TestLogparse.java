import com.jmx.analysis.LogParse;
import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/8/27
 *  @Time: 10:23
 *  
 */
public class TestLogparse {
    public static void main(String[] args) {


        String log = "192.168.10.1 - - [27/Aug/2020:10:20:53 +0800] \"GET /forum.php?mod=viewthread&tid=9&extra=page%3D1 HTTP/1.1\" 200 39913 \"http://kms-4/forum.php?mod=forumdisplay&fid=41\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36\"";

        LogParse parse = new LogParse();

        AccessLogRecord accessLogRecord = parse.parseRecord(log);

       // System.out.println(accessLogRecord.getRequest());

        // 匹配出前面是"forumdisplay&fid="的数字记为版块id
        String sectionIdRegex = "(\\?mod=forumdisplay&fid=)(\\d+)";
        Pattern sectionPattern = Pattern.compile(sectionIdRegex);
        // 匹配出前面是"tid="的数字记为文章id
        String articleIdRegex = "(\\?mod=viewthread&tid=)(\\d+)";
        Pattern articlePattern = Pattern.compile(articleIdRegex);

        String[] arr = accessLogRecord.getRequest().split(" ");
        String sectionId = "";
        String articleId = "";
        if (arr.length == 3) {
            //System.out.println(arr[1]);

            Matcher sectionMatcher = sectionPattern.matcher(arr[1]);
            Matcher articleMatcher = articlePattern.matcher(arr[1]);
            //System.out.println(articleMatcher.find());
           // System.out.println(sectionMatcher.find());
            //System.out.println(articleMatcher.group(0));
            //System.out.println(articleMatcher.group(1));
            //System.out.println(articleMatcher.group(2));

        /*    sectionId = (sectionMatcher.find()) ? sectionMatcher.group(2) : "";
            articleId = articleMatcher.find() ? articleMatcher.group(2) : "";*/

           /* if (articleMatcher.find()){
                articleId = articleMatcher.group(2);
            } else {
                articleId = "no";
            }
            if (sectionMatcher.find()){
                sectionId = sectionMatcher.group(2);
            }else{
                sectionId = "no";
            }
*/
        }
     /*   System.out.println( articleId);
        System.out.println(sectionId);*/
       Tuple2<String, String> stringStringTuple2 = parse.parseSectionIdAndArticleId(accessLogRecord.getRequest());
        System.out.println(stringStringTuple2.f0 + stringStringTuple2.f1);
        System.out.println(stringStringTuple2);
    }
}
