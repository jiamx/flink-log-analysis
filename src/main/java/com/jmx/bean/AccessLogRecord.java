package com.jmx.bean;

import lombok.Data;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/8/24
 *  @Time: 21:41
 *  
 */

/**
 * 原始日志封装类
 */
@Data
public class AccessLogRecord {
    public String clientIpAddress; // 客户端ip地址
    public String clientIdentity; // 客户端身份标识,该字段为 `-`
    public String remoteUser; // 用户标识,该字段为 `-`
    public String dateTime; //日期,格式为[day/month/yearhourminutesecond zone]
    public String request; // url请求,如：`GET /foo ...`
    public String httpStatusCode; // 状态码，如：200; 404.
    public String bytesSent; // 传输的字节数，有可能是 `-`
    public String referer; // 参考链接,即来源页
    public String userAgent;  // 浏览器和操作系统类型
}
