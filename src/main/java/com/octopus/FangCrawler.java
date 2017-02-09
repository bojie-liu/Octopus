package com.octopus;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.annotation.ThreadSafe;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.utils.FilePersistentBase;
import us.codecraft.webmagic.selector.Selectable;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

/**
 * Created by joshualiu on 2/6/17.
 */

@ThreadSafe
class FangPipeline extends FilePersistentBase implements Pipeline {
    private org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());
    private String filename;

    private String GetCurrentTime() {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-d-HH-mm-ss");
        return formatter.format(date);
    }

    public FangPipeline(String path) {
        this.setPath(path);
        this.filename = GetCurrentTime();
    }

    public void process(ResultItems resultItems, Task task) { 
        String path = this.path + PATH_SEPERATOR;

        try {
            PrintWriter fd = new PrintWriter(new OutputStreamWriter(new FileOutputStream(this.getFile(path + this.filename + ".txt"), true), "UTF-8"));
            for (Map.Entry e:
                 resultItems.getAll().entrySet()) {
                if (e.getKey() == "district")
                    continue;

                fd.println((String)e.getValue());
            }
            fd.close();
            logger.info("Save " + Integer.toString(resultItems.getAll().size() - 1) + " items.");
        } catch (IOException var10) {
            this.logger.warn("write file error", var10);
        }

    }
}

class FangProcessor implements PageProcessor {
    private static final String rootUrlRegex = "http://esf.sz.fang.com/housing/87_[0-9]{0,}_1_0_0_0_[0-9]{1,}_0_0/";
    public static final Map<String, String> rootUrlList;
    static {
        rootUrlList = new HashMap<String, String>();
        rootUrlList.put("http://esf.sz.fang.com/housing/87_341_1_0_0_0_1_0_0/", "西丽");
        rootUrlList.put("http://esf.sz.fang.com/housing/87_342_1_0_0_0_1_0_0/", "南头");
        rootUrlList.put("http://esf.sz.fang.com/housing/87_340_1_0_0_0_1_0_0/", "科技园");
    }

    private final int maxArticleNum = 10000;
    private static int articleNum = 0;

    private static Logger log = Logger.getLogger(FangProcessor.class);
    private Site site = Site.me().setRetryTimes(5).setSleepTime(10000);

    private String concat(List<List<String>> table, int i) {
        String rlt = new String();
        try {
            for (List<String> col:
                 table) {
                rlt += col.get(i) + " ";
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Concat out of bound!");
        }
        return rlt;
    }

    @Override
    public void process(Page page) {
        if (articleNum >= maxArticleNum) {
            page.getTargetRequests().clear();
            page.setSkip(true);
            return;
        }

        String district = rootUrlList.get(page.getUrl().get());
        if (district != null && !district.isEmpty()) {
            page.putField("district", district);
        }

        if (page.getUrl().regex(rootUrlRegex).match()) {
            String pageIndexUrl = page.getHtml().xpath("//a[@id='PageControl1_hlk_next']").links().get();
            page.addTargetRequest(pageIndexUrl);
            log.info("Adding request " + pageIndexUrl);

            List<String> houseName = page.getHtml().xpath("//a[@class='plotTit']/text()").all();
            List<String> houseUrl = page.getHtml().xpath("//a[@class='plotTit']").links().all();
            List<String> pageDistrict = page.getHtml().xpath("//dl[@class='plotListwrap clearfix']/dd/p[2]/a[1]/text()").all();
            List<String> area = page.getHtml().xpath("//dl[@class='plotListwrap clearfix']/dd/p[2]/a[2]/text()").all();
            List<String> address = page.getHtml().xpath("//dl[@class='plotListwrap clearfix']/dd/p[2]/text()").all();
            List<String> buildTime = page.getHtml().xpath("//ul[@class='sellOrRenthy clearfix']/li[3]/text()").all();
//            List<String> avgPrice = page.getHtml().xpath("//p[@class='priceAverage']/span[1]/text()").all();
//            List<String> ratio = page.getHtml().xpath("//p[@class='ratio']/span/text()").all();
            List<Selectable> priceNodes = page.getHtml().xpath("//div[@class='listRiconwrap']").nodes();
            List<String> avgPrice = new ArrayList<String>();
            List<String> ratio = new ArrayList<String>();

            for (Selectable node:
                 priceNodes) {
                String price = node.xpath("//p[@class='priceAverage']/span[1]/text()").get();
                if (price != null)
                    avgPrice.add(price);
                else
                    avgPrice.add("0");

                String r = node.xpath("//p[@class='ratio']/span/text()").get();
                if (r != null)
                    ratio.add(r);
                else
                    ratio.add("0");
            }

            for (int i = 0; i < ratio.size(); ++i) {
                String kindRatio = ratio.get(i).replace("↓", "-").replace("↑", "");
                ratio.set(i, kindRatio);
            }

            for (int i = 0; i < buildTime.size(); ++i) {
                String kindBuildTime = buildTime.get(i).replace("年建成", "");
                buildTime.set(i, kindBuildTime);
            }

            for (int i = 0; i < address.size(); ++i) {
                String kindAddress = address.get(i).replace(" ", "").replace("-", "");
                address.set(i, kindAddress);
            }

            for (int i = 0; i < houseName.size(); i++) {
                page.putField(Integer.toString(i+1),
                        this.concat(Arrays.asList(houseName, pageDistrict, area, address, buildTime, avgPrice, ratio, houseUrl), i));
            }
            log.info("Process " + Integer.toString(page.getResultItems().getAll().size() - 1) + " items.");
        } else {
        }

    }

    @Override
    public Site getSite() {
        return site;
    }
}

public class FangCrawler {
    public static void main(String[] args) {
        for (Map.Entry<String, String> entry : FangProcessor.rootUrlList.entrySet()
             ) {
            Spider.create(new FangProcessor())
                    .addPipeline(new FangPipeline("/Users/joshualiu/dev/tmp/fang/"))
                    .addUrl(entry.getKey()).run();
        }
    }
}
