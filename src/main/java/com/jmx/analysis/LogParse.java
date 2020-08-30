package com.jmx.analysis;

import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/8/24
 *  @Time: 22:21
 *  
 */
public class LogParse implements Serializable {

    //构建正则表达式
    private String regex = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (\\S+) (\\S+) (\\[.+?\\]) (\\\"(.*?)\\\") (\\d{3}) (\\S+) (\\\"(.*?)\\\") (\\\"(.*?)\\\")";
    private Pattern p = Pattern.compile(regex);

    /*
     *构造访问日志的封装类对象
     * */
    public AccessLogRecord buildAccessLogRecord(Matcher matcher) {
        AccessLogRecord record = new AccessLogRecord();
        record.setClientIpAddress(matcher.group(1));
        record.setClientIdentity(matcher.group(2));
        record.setRemoteUser(matcher.group(3));
        record.setDateTime(matcher.group(4));
        record.setRequest(matcher.group(5));
        record.setHttpStatusCode(matcher.group(6));
        record.setBytesSent(matcher.group(7));
        record.setReferer(matcher.group(8));
        record.setUserAgent(matcher.group(9));
        return record;

    }

    /**
     * @param record:record表示一条apache combined 日志
     * @return 解析日志记录，将解析的日志封装成一个AccessLogRecord类
     */
    public AccessLogRecord parseRecord(String record) {
        Matcher matcher = p.matcher(record);
        if (matcher.find()) {
            return buildAccessLogRecord(matcher);
        }
        return null;
    }

    /**
     * @param request url请求，类型为字符串，类似于 "GET /the-uri-here HTTP/1.1"
     * @return 一个三元组(requestType, uri, httpVersion). requestType表示请求类型，如GET, POST等
     */
    public Tuple3<String, String, String> parseRequestField(String request) {
        //请求的字符串格式为：“GET /test.php HTTP/1.1”，用空格切割
        String[] arr = request.split(" ");
        if (arr.length == 3) {
            return Tuple3.of(arr[0], arr[1], arr[2]);
        } else {
            return null;
        }

    }

    /**
     * 将apache日志中的英文日期转化为指定格式的中文日期
     *
     * @param dateTime 传入的apache日志中的日期字符串，"[21/Jul/2009:02:48:13 -0700]"
     * @return
     */
    public String parseDateField(String dateTime) throws ParseException {
        // 输入的英文日期格式
        String inputFormat = "dd/MMM/yyyy:HH:mm:ss";
        // 输出的日期格式
        String outPutFormat = "yyyy-MM-dd HH:mm:ss";

        String dateRegex = "\\[(.*?) .+]";
        Pattern datePattern = Pattern.compile(dateRegex);

        Matcher dateMatcher = datePattern.matcher(dateTime);
        if (dateMatcher.find()) {
            String dateString = dateMatcher.group(1);
            SimpleDateFormat dateInputFormat = new SimpleDateFormat(inputFormat, Locale.ENGLISH);
            Date date = dateInputFormat.parse(dateString);

            SimpleDateFormat dateOutFormat = new SimpleDateFormat(outPutFormat);

            String formatDate = dateOutFormat.format(date);
            return formatDate;
        } else {
            return "";
        }
    }

    /**
     * 解析request,即访问页面的url信息解析
     * "GET /about/forum.php?mod=viewthread&tid=5&extra=page%3D1 HTTP/1.1"
     * 匹配出访问的fid:版本id
     * 以及tid：文章id
     *
     * @param request
     * @return
     */
    public Tuple2<String, String> parseSectionIdAndArticleId(String request) {
        // 匹配出前面是"forumdisplay&fid="的数字记为版块id
        String sectionIdRegex = "(\\?mod=forumdisplay&fid=)(\\d+)";
        Pattern sectionPattern = Pattern.compile(sectionIdRegex);
        // 匹配出前面是"tid="的数字记为文章id
        String articleIdRegex = "(\\?mod=viewthread&tid=)(\\d+)";
        Pattern articlePattern = Pattern.compile(articleIdRegex);

        String[] arr = request.split(" ");
        String sectionId = "";
        String articleId = "";
        if (arr.length == 3) {
            Matcher sectionMatcher = sectionPattern.matcher(arr[1]);
            Matcher articleMatcher = articlePattern.matcher(arr[1]);
                sectionId = (sectionMatcher.find()) ? sectionMatcher.group(2) : "";
                articleId = (articleMatcher.find()) ? articleMatcher.group(2) : "";

        }
        return  Tuple2.of(sectionId, articleId);

    }

}
