## 文件描述
* crawler.py: Crawler组件实现
* loader.py: 加载器实现
* fetcher/*: 爬取器
* fetcher/base_crawler.py: 爬取器父类定义
* fetcher/icourse/*: iCourse爬取器实现
* fetcher/xueyinonline/*: 学银在线爬取器实现

## 爬取器
* 接收一个WebDriver，此Driver已经恢复登录态，并已经打开了待爬取的URL
* 返回一个dict，包含了所有爬取到的信息的K-V对