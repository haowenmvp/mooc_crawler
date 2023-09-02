import time

from requests_html import HTMLSession
from fake_useragent import UserAgent


class ProxyIP(object):
    def __init__(self):
        self.ipPool = []
        self.effectiveIpPool = []
        self.XiCiHeaders = {
            "Host": "www.xicidaili.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cookie": "_free_proxy_session"
                      "=BAh7B0kiD3Nlc3Npb25faWQGOgZFVEkiJTZmNzQyYjUxZjQ4NWM5ZGFiZjdk"
                      "ZTMxOWVhNTM2ZDJiBjsAVEkiEF9jc3JmX3Rva2VuBjsARkkiMVZ2U0xHNkYwRkR"
                      "odnFQREZ2THMwRUgwellmbWp0K1ZYTXlmVmcwZ1ZZb0U9BjsARg%3D%3D--780471f"
                      "08d5ee01db83720b7813087d4f2ce7528; Hm_lvt_0cf76c77469e965d2957f0553e6ecf59=1582770627; "
                      "Hm_lpvt_0cf76c77469e965d2957f0553e6ecf59=" + str(self.getLocalTime()),
            "Upgrade-Insecure-Requests": "1",
            "If-None-Match": "W/'032da28335ebdfc2ed50455b3135d783'",
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1"
        }
        self.KuaiDaiLiHeaders = {
            "Host": "www.kuaidaili.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.kuaidaili.com/free/",
            "Connection": "keep-alive",
            "Cookie": "channelid=0; sid=1582783569316649; _ga=GA1.2.1408732808.1582784092; _gid=GA1.2.468389076.1582784092; _gat=1; Hm_lvt_7ed65b1cc4b810e9fd37959c9bb51b31=1582784092; Hm_lpvt_7ed65b1cc4b810e9fd37959c9bb51b31=1582784103",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }
        pass

    def crawlIp(self):
        #self.xiCiProxy()
        self.kuaiDaiLi()
        pass

    def xiCiProxy(self):
        session1 = HTMLSession()
        ua = UserAgent()
        self.XiCiHeaders["User-Agent"] = ua.random
        self.XiCiHeaders["Cookie"] = "_free_proxy_session" \
                                     "=BAh7B0kiD3Nlc3Npb25faWQGOgZFVEkiJTZmNzQyYjUxZjQ4" \
                                     "NWM5ZGFiZjdkZTMxOWVhNTM2ZDJiBjsAVEkiEF9jc3JmX3Rva2VuBjsARkkiMVZ2U0xHNkYwRkRo" \
                                     "dnFQREZ2THMwRUgwellmbWp0K1ZYTXlmVmcwZ1ZZb0U9BjsARg%3D%3D--780471f08d5ee01db83720b7813" \
                                     "087d4f2ce7528; Hm_lvt_0cf76c77469e965d2957f0553e6ecf59=1582770627; Hm_lpvt_0cf76c77469e965d2957f0553e6ecf59=" + str(
            self.getLocalTime())
        r1 = session1.get("https://www.xicidaili.com/nn/1", headers=self.XiCiHeaders, timeout=60)
        print(r1.status_code)
        ipList = r1.html.find("table#ip_list", first=True)
        ipList2 = ipList.find("tr.odd")
        for i in range(0, 10):
            ip_address = ipList2[i].find("td")[1].text
            port = ipList2[i].find("td")[2].text
            type = ipList2[i].find("td")[5].text
            ip = {
                "address": ip_address,
                "port": port,
                "type": type
            }
            self.ipPool.append(ip)
        r1.close()
        session1.close()
        pass

    def kuaiDaiLi(self):
        session2 = HTMLSession()
        ua = UserAgent()
        self.XiCiHeaders["User-Agent"] = ua.random
        r2 = session2.get("https://www.kuaidaili.com/free/", headers=self.KuaiDaiLiHeaders, timeout=30)
        table = r2.html.find("table.table.table-bordered.table-striped", first=True)
        ipList = table.find("tbody>tr")
        for i in range(0, 6):
            ip_address = ipList[i].find("td")[0].text
            port = ipList[i].find("td")[1].text
            type = ipList[i].find("td")[3].text
            ip = {
                "address": ip_address,
                "port": port,
                "type": type
            }
            self.ipPool.append(ip)
        r2.close()
        session2.close()
        pass

    def verify(self):
        for item in self.ipPool:
            proxy = {item["type"]: item["address"] + ":" + item["port"]}
            session2 = HTMLSession()
            r2 = session2.get("https://www.baidu.com/", proxies=proxy)
            if r2.status_code == 200:
                self.effectiveIpPool.append(item)
            r2.close()
            session2.close()
        pass

    def getLocalTime(self):
        now = time.time()
        return int(now)

    def run(self):
        self.crawlIp()
        self.verify()
        for item in self.effectiveIpPool:
            print(item)
        return self.effectiveIpPool
        pass


if __name__ == '__main__':
    crawler = ProxyIP()
    crawler.run()
