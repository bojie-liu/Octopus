package com.octopus;

import com.alibaba.fastjson.JSON;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.pipeline.JsonFilePipeline;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.utils.FilePersistentBase;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by joshualiu on 1/17/17.
 */

class JQKAPipeline extends FilePersistentBase implements Pipeline, Closeable {
    private org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());


    private int mMaxBufSize = 1024 * 1024;
    private ResultItems mHeader = new ResultItems();
    private List<Map<String, Object>> mItems;
    private String mUUID = new String();

    public JQKAPipeline(String path) {
        mItems = new ArrayList<Map<String, Object>>(mMaxBufSize);
        this.setPath(path);
    }

    @Override
    public void close() {
        if (mItems.isEmpty())
            return;

        ResultItems corpus = new ResultItems();
        corpus.setRequest(mHeader.getRequest());
        corpus.put("domain", mHeader.getRequest().getUrl());
        corpus.put("num", mItems.size());
        corpus.put("contents", mItems);

        String path = this.path + PATH_SEPERATOR + mUUID + PATH_SEPERATOR;
        try {
            PrintWriter e = new PrintWriter(new FileWriter(this.getFile(path + DigestUtils.md5Hex(corpus.getRequest().getUrl()) + ".json")));
            e.write(JSON.toJSONString(corpus.getAll()));
            e.close();
        } catch (IOException var5) {
            this.logger.warn("write file error", var5);
        }
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        if (resultItems.isSkip())
            return;

        if (mUUID.isEmpty())
            mUUID = task.getUUID();

        if (mHeader.getAll().isEmpty())
            mHeader = resultItems;

        mItems.add(resultItems.getAll());
    }
}

public class JQKACrawler implements PageProcessor {
    public final String rootUrl = "http://stock.10jqka.com.cn/bkfy_list/index_[0-9]{1,}.shtml";
    private final int maxArticleNum = 10;
    private static int articleNum = 0;

    private static Logger log = Logger.getLogger(JQKACrawler.class);
    private Site site = Site.me().setRetryTimes(5).setSleepTime(1000);

    private static int pageListIndex = 1;
    public static String getNextPageListUrl() {
        String url = "http://stock.10jqka.com.cn/bkfy_list/index_" +
                Integer.toString(pageListIndex++) + ".shtml";
        return url;
    }

    @Override
    public void process(Page page) {
        if (articleNum >= maxArticleNum) {
            page.getTargetRequests().clear();
            page.setSkip(true);
            return;
        }

        if (page.getUrl().regex(rootUrl).match()) {
            List<String> articleUrls = page.getHtml().xpath("//span[@class='arc-title']").links().all();
            page.addTargetRequests(articleUrls);
            page.addTargetRequest(JQKACrawler.getNextPageListUrl());
            page.setSkip(true);
//            log.info(page.getUrl().get());
        } else {
                String keywords = page.getHtml().xpath("//meta[@name='keywords']/@content").get();
                String head = page.getHtml().xpath("//div[@class='atc-head']/h1/text()").get();
                String time = page.getHtml().xpath("//span[@id='pubtime_baidu']/text()").get();
                List<String> contentList = page.getHtml().xpath("//div[@class='atc-content']").smartContent().all();
                String contents = String.join("", contentList.toArray(new String[contentList.size()]));

                page.putField("url", page.getUrl().get());
                page.putField("keywords", keywords);
                page.putField("title", head);
                page.putField("time", time);
                page.putField("contents", contents);
                articleNum++;
                log.info("processing article: " + Integer.toString(articleNum));
        }

    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new JQKACrawler())
                .addPipeline(new JQKAPipeline("/Users/joshualiu/dev/tmp/"))
                .addUrl(JQKACrawler.getNextPageListUrl()).run();
    }
}
