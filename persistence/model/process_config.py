class ProcessConfig:
    def __init__(self):
        self.platform_dict = {
            "爱课程(中国大学MOOC)": {
                "platform_processor": "data_process.platform_processors.icourse163.ProcessorPlatformCourse"
            },
            "安徽省网络课程学习中心平台": {
                "platform_processor": "data_process.platform_processors.ehuixue.ProcessorPlatformCourse"
            },
            "北京超星尔雅教育科技有限公司": {
                "platform_processor": "data_process.platform_processors.erya.ProcessorPlatformCourse"
            },
            "北京高校邦科技有限公司": {
                "platform_processor": "data_process.platform_processors.gaoxiaobang.ProcessorPlatformCourse"
            },
            "北京高校优质课程研究会": {
                "platform_processor": "data_process.platform_processors.livedu.ProcessorPlatformCourse"
            },
            "优学院（人民网公开课）": {
                "platform_processor": "data_process.platform_processors.ulearning.ProcessorPlatformCourse"
            },
            "北京学银在线教育科技有限公司": {
                "platform_processor": "data_process.platform_processors.xueyinonline.ProcessorPlatformCourse"
            },
            "重庆高校在线开放课程平台（重庆市教育委员会）": {
                "platform_processor": "data_process.platform_processors.cqooc.ProcessorPlatformCourse"
            },
            "国家开放大学出版社有限公司（荟学习网）": {
                "platform_processor": "data_process.platform_processors.huixuexi.ProcessorPlatformCourse"
            },
            "好大学在线": {
                "platform_processor": "data_process.platform_processors.haodaxue.ProcessorPlatformCourse"
            },
            "华文慕课": {
                "platform_processor": "data_process.platform_processors.chinesemooc.ProcessorPlatformCourse"
            },
            "人卫社MOOC": {
                "platform_processor": "data_process.platform_processors.pmphmooc.ProcessorPlatformCourse"
            },
            "智慧树": {
                "platform_processor": "data_process.platform_processors.zhihuishu.ProcessorPlatformCourse"
            },
            "优慕课": {
                "platform_processor": "data_process.platform_processors.umooc.ProcessorPlatformCourse"
            },
            "玩课网": {
                "platform_processor": "data_process.platform_processors.wanke.ProcessorPlatformCourse"
            },
            "微知库数字校园学习平台": {
                "platform_processor": "data_process.platform_processors.weizhiku.ProcessorPlatformCourse"
            },
            "学堂在线": {
                "platform_processor": "data_process.platform_processors.xuetangx.ProcessorPlatformCourse"
            },
            "优课联盟": {
                "platform_processor": "data_process.platform_processors.youkelianmeng.ProcessorPlatformCourse"
            },
            "浙江省高等学校在线开放课程共享平台": {
                "platform_processor": "data_process.platform_processors.zhejiangmooc.ProcessorPlatformCourse"
            },
            "智慧职教": {
                "platform_processor": "data_process.platform_processors.zhihuizhijiao.ProcessorPlatformCourse"
            },
            "中国高校外语慕课平台": {
                "platform_processor": "data_process.platform_processors.gaoxiaowaiyumuke.ProcessorPlatformCourse"
            },
            "中科云教育": {
                "platform_processor": "data_process.platform_processors.zhongkeyun.ProcessorPlatformCourse"
            }

        }
        self.repo = {
            "host": "192.168.232.111",
            "port": 3307,
            "username": "root",
            "password": "123qweASD!@#",
            "database": "mooc_test"
        }
