package com.octopus;

import org.apache.log4j.Logger;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.JsonFilePipeline;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.List;

/**
 * Created by joshualiu on 1/17/17.
 */
public class JQKACrawler implements PageProcessor{
    public final String rootUrl = "http://stock.10jqka.com.cn/bkfy_list/index_[0-9]{1,}.shtml";
    private final int maxArticleNum = 2;
    private static int articleNum = 0;

    private static Logger log = Logger.getLogger(JQKACrawler.class);
    private Site site = Site.me().setRetryTimes(5).setSleepTime(1000);

    @Override
    public void process(Page page) {
        if (page.getUrl().regex(rootUrl).match()) {
            List<String> articleUrls = page.getHtml().xpath("//span[@class='arc-title']").links().all();
            page.addTargetRequests(articleUrls);
//            log.info(page.getUrl().get());
        } else {
            if (articleNum < maxArticleNum) {
                String keywords = page.getHtml().xpath("//meta[@name='keywords']/@content").get();
                String head = page.getHtml().xpath("//div[@class='atc-head']/h1/text()").get();
                List<String> contentList = page.getHtml().xpath("//div[@class='atc-content']").smartContent().all();
                String contents = String.join("", contentList.toArray(new String[contentList.size()]));

                page.putField("url", page.getUrl().get());
                page.putField("keywords", keywords);
                page.putField("title", head);
                page.putField("contents", contents);
                articleNum++;
            }
        }

    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new JQKACrawler())
                .addPipeline(new JsonFilePipeline("/Users/joshualiu/dev/tmp/"))
                .addUrl("http://stock.10jqka.com.cn/bkfy_list/index_1.shtml").run();
    }
}
