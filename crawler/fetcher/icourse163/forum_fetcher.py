import logging
import random
import re
import string
import time
from datetime import datetime
from crawler.fetcher.base_fetcher import BaseFetcher
import requests
from requests_html import HTML
from retrying import retry
from persistence.model.forum import ForumPostInfo


class ForumFetcher(BaseFetcher):
    def __init__(self,):
        super().__init__()
        self.courseUrl = ''
        self.httpSessionId = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.tid = ''
        self.mainTipList = []
        self.replyTipList = []
        self.commentTipList = []
        self.error_list = []
        pass

    def dealCommentPost(self, forum_name, subject, replyTipId, content):
        """
        :param mainTipId:
        :param replyTipId:
        :param content:
        :return: 处理结果返回至self.commentTipList列表
        """
        content = str(content)
        allCommentor = re.findall(r's\d+\.commentorId=\d+', content)
        for perComment in allCommentor:
            try:
                commentVarName = str(perComment).split('.')[0]
                comment_forum_post_info = ForumPostInfo()
                comment_forum_post_info.platform = '爱课程(中国大学MOOC)'
                comment_forum_post_info.forum_reply_id = replyTipId
                comment_forum_post_info.update_time = datetime.now()
                comment_forum_post_info.forum_post_type = 2
                comment_forum_post_info.forum_subject = subject
                comment_forum_post_info.forum_name = forum_name
                shortIntroduction = re.search(commentVarName + r'\.content="(.*)";', content, re.S).group().split('"')[1]
                if re.search(u'[\u4E00-\u9FFF]+', shortIntroduction, re.S) == None:
                    shortIntroduction = str(
                        re.search(commentVarName + r'\.content="(.*)";' + commentVarName + '\.countVote', content,
                                  re.S).group()).replace(commentVarName + '.content="', '').replace(
                        "\";" + commentVarName + ".countVote", '')
                html = HTML(html=shortIntroduction)
                comment_forum_post_info.forum_post_content = html.text
                if comment_forum_post_info.forum_post_content == '':
                    result = re.findall(u'[\u4E00-\u9FFF]+', shortIntroduction, re.S)
                    for perChinese in result:
                        comment_forum_post_info.forum_post_content = comment_forum_post_info.forum_post_content + perChinese
                comment_forum_post_info.forum_post_content = re.sub(r'\n|&nbsp|\xa0|&gt|&lt|\t|\'D\'|\\n', '',
                                                          comment_forum_post_info.forum_post_content)
                comment_forum_post_info.forum_post_content = self.dealCharacter(comment_forum_post_info.forum_post_content)
                # print(commentTipInfo.shortIntroduction)
                try:
                    commentTime = re.search(commentVarName + r'\.commentTime=\d+;', content)
                    if commentTime == None:
                        commentTime = re.search(commentVarName + r'\.commentTime=\d+', content, re.S)
                    commentTime = commentTime.group().split('=')[1].replace(";", '').replace("\n", '').replace("&nbsp", '')
                    timestamp = int(int(commentTime) / 1000)
                    comment_forum_post_info.forum_post_time = datetime.fromtimestamp(timestamp)
                except:
                    print("爬取评论时间时出错" + self.courseUrl + ":" + replyTipId)
                    description = "爬取评论时间时出错 courseUrl:" + self.courseUrl + " replyTipId:" + replyTipId
                    dict = {'logTime': datetime.now(), 'platform': '爱课程（中国大学mooc）',
                            'description': description}
                    logging.warning(dict)
                    pass
                commentor = re.search(commentVarName + r'\.commentor=s\d+', content).group().split('=')[1]
                comment_forum_post_info.forum_reply_userid = re.search(commentor + r'\.id=\d+', content).group().split('=')[1]
                comment_forum_post_info.forum_post_username = re.search(commentor + r'.nickName=".*";', content).group().split('"')[1]
                comment_forum_post_info.forum_post_username = self.dealCharacter(comment_forum_post_info.forum_post_username)
                commentRole = re.search(commentor + r'\.roles=[0-9a-z]+', content).group().split('=')[1]
                if commentRole == 'null':
                    comment_forum_post_info.forum_post_userrole = 2
                else:
                    posterRole = \
                    re.search(commentRole + r'\[0\]="[0-9a-zA-Z]+"', content).group().split('"')[1]
                    if posterRole == 'lector':
                        comment_forum_post_info.forum_post_userrole = 1
                    elif posterRole == 'assistant':
                        comment_forum_post_info.forum_post_userrole = 3
                    else:
                        comment_forum_post_info.forum_post_userrole = 0
                comment_forum_post_info.forum_post_id = re.search(commentVarName + r'\.id=\d+', content).group().split('=')[1]
                self.commentTipList.append(comment_forum_post_info.__dict__)
                print(comment_forum_post_info.__dict__)
            except:
                pass
        pass

    def dealReplyPost(self, forum_name, subject, mainTipId, content):
        """
        :param content:
        :return: 处理结果返回至self.replyTipList列表   其中commentId为空
        """
        content = str(content)
        allResponder = re.findall(r's\d+\.replyerId=\d+', content)
        for oneResponder in allResponder:
            try:
                reply_forum_post_info = ForumPostInfo()
                reply_forum_post_info.platform = '爱课程(中国大学MOOC)'
                reply_forum_post_info.forum_reply_id = mainTipId
                replyVarName = str(oneResponder).split('.')[0]
                shortIntroduction = re.search(replyVarName + r'\.content="(.*)";', content, re.S).group().split('"')[1]
                # replyTipInfo.shortIntroduction = replyContent_html.replace("<p>", '').replace("</p>", '').replace("<br >", '').replace("<br />", '').replace("<div>", '').replace("/div", '')
                if re.search(u'[\u4E00-\u9FFF]+', shortIntroduction, re.S) == None:
                    shortIntroduction = re.search(replyVarName + r'\.content="(.*)";' + replyVarName + '\.countComment',
                                                  content, re.S).group().replace(replyVarName + '.content="', '').replace(
                        "\";" + replyVarName + ".countComment", '')
                html = HTML(html=shortIntroduction)
                reply_forum_post_info.forum_post_content = html.text
                if reply_forum_post_info.forum_post_content == '':
                    result = re.findall(u'[\u4E00-\u9FFF]+', shortIntroduction, re.S)
                    for perChinese in result:
                        reply_forum_post_info.forum_post_content = reply_forum_post_info.forum_post_content + perChinese
                # replyTipInfo.shortIntroduction = str(replyTipInfo.shortIntroduction).strip().replace(u'\n', u'', re.S).replace(u'&nbsp', u'', re.S).replace(u'\xa0', u'', re.S).replace(u'&gt', u'', re.S).replace(u'&lt', u'', re.S).replace(u"\t", u'', re.S)
                reply_forum_post_info.forum_post_content = re.sub(r'\n|&nbsp|\xa0|&gt|&lt|\t|\'D\'|\\n', '',
                                                        reply_forum_post_info.forum_post_content)
                reply_forum_post_info.forum_post_content = self.dealCharacter(reply_forum_post_info.forum_post_content)
                # print(replyTipInfo.shortIntroduction)
                replyTime = re.search(replyVarName + r'\.replyTime=(\d+);', content).group().split("=")[1].replace(';', '')
                timestamp = int(int(replyTime) / 1000)
                reply_forum_post_info.forum_post_time = datetime.fromtimestamp(timestamp)
                reply_forum_post_info.forum_reply_userid = re.search(replyVarName + r'\.replyerId=(\d+);', content).group().split("=")[
                    1].replace(";", '')
                replyer = re.search(replyVarName + r'\.replyer=s\d+;', content).group().split("=")[1].replace(";", '')
                posterNickName = re.search(replyer + r'\.nickName=".*";', content)
                if posterNickName == None:
                    posterNickName = re.search(replyer + r'\.nickName=".*";', content, re.S).group().split('"')[1].replace(
                        ";",
                        '')
                else:
                    posterNickName = posterNickName.group().split('"')[1].replace(";", '')
                reply_forum_post_info.forum_post_username = posterNickName
                reply_forum_post_info.forum_post_username = self.dealCharacter(reply_forum_post_info.forum_post_username)
                replyerRole = re.search(replyer + r'\.roles=[0-9a-z]+;', content).group().split('=')[1].replace(";", '')
                if replyerRole == 'null':
                    reply_forum_post_info.forum_post_userrole = 2
                else:
                    replyer_role_pattern = replyerRole + r'\[0\]="[0-9a-zA-Z]+"'
                    posterRole = str(re.search(replyer_role_pattern, content).group()).split('"')[1]
                    if posterRole == 'lector':
                        reply_forum_post_info.forum_post_userrole = 1
                    elif posterRole == 'assistant':
                        reply_forum_post_info.forum_post_userrole = 3
                    else:
                        reply_forum_post_info.forum_post_userrole = 0
                reply_forum_post_info.forum_post_id = re.search(replyVarName + r'\.id=\d+;', content).group().split('=')[1].replace(";",
                                                                                                                     '')
                replyCount = re.search(replyVarName + r'\.countComment=\d+;', content).group().split('=')[
                    1].replace(";", '')
                reply_forum_post_info.forum_post_type = 2
                reply_forum_post_info.update_time = datetime.now()
                reply_forum_post_info.forum_subject = subject
                reply_forum_post_info.forum_name = forum_name
                reply_forum_post_info_dict = reply_forum_post_info.__dict__
                reply_forum_post_info_dict["replyCount"] = replyCount
                self.replyTipList.append(reply_forum_post_info_dict)
                print(reply_forum_post_info_dict)
            except :
                pass
        pass

    def dealMainPost(self, content):
        """
        通过正则表达式获取content中的每条帖子的s变量，以此获取该贴其他内容
        :param content:
        :return: 处理结果返回至self.mainTipList列表

        """
        content = str(content)
        all_titles = re.findall(r's\d+\.title=".*";', content)  # 匹配title
        for one_title in all_titles:
            main_forum_post_info = ForumPostInfo()
            main_forum_post_info.platform = '爱课程(中国大学MOOC)'
            tip_var_name = one_title.split('.')[0]  # 每个主帖的变量名称
            main_forum_post_info.forum_post_id = (re.search(tip_var_name + r'\.id=(\d+);', content)).group().replace(
                tip_var_name + '.id=', '').replace(';', '')
            # main_forum_post_info.forum_post_id = str(uuid.uuid1())
            main_forum_post_info.forum_name = self.get_forum_name(main_forum_post_info.forum_post_id)
            main_forum_post_info.forum_subject = re.search(tip_var_name + r'\.title="(.*)";', content).group().replace(
                tip_var_name + '.title=', '').replace(';', '').replace('\"', '').replace("\u200b", '')
            main_forum_post_info.forum_subject = self.dealCharacter(main_forum_post_info.forum_subject)
            postTime = re.search(tip_var_name + r'\.postTime=(\d+);', content).group().replace(
                tip_var_name + '.postTime=', '').replace(';', '')
            timeStamp = int(int(postTime) / 1000)
            main_forum_post_info.forum_post_time = datetime.fromtimestamp(timeStamp)
            replyCount = re.search(tip_var_name + r'\.countReply=(\d+);', content).group().replace(
                tip_var_name + '.countReply=', '').replace(';', '')
            poster_var_name = re.search(tip_var_name + r'\.poster=(s\d+);', content).group().replace(
                tip_var_name + '.poster=', '').replace(';', '')
            main_forum_post_info.forum_reply_userid = re.search(poster_var_name + r'\.id=(\d+);', content).group().replace(
                poster_var_name + '.id=', '').replace(';', '')
            main_forum_post_info.forum_post_username = \
                re.search(poster_var_name + r'\.nickName="(.*)";', content).group().split('"')[1]
            main_forum_post_info.forum_post_username = self.dealCharacter(main_forum_post_info.forum_post_username)
            postRole = re.search(poster_var_name + r'\.roles=[0-9a-z]+;', content).group().replace(
                poster_var_name + '.roles=', '').replace(';', '')
            if postRole == 'null':
                main_forum_post_info.forum_post_userrole = 2
            else:
                posterRole = re.search(postRole + r'\[0\]="[0-9a-zA-Z]+";', content).group().replace(
                    postRole + '[0]=', '').replace(';', '').replace('\"', '')
                if posterRole == 'lector':
                    main_forum_post_info.forum_post_userrole = 1
                elif posterRole == 'assistant':
                    main_forum_post_info.forum_post_userrole = 3
            main_forum_post_info.forum_post_type = 1
            main_forum_post_info.forum_reply_id = '0'
            main_forum_post_info.update_time = datetime.now()
            main_forum_post_info_dict = main_forum_post_info.__dict__
            main_forum_post_info_dict["replyCount"] = replyCount
            self.mainTipList.append(main_forum_post_info_dict)
            print(main_forum_post_info.__dict__)
        pass

    def CrawlMainTip(self):
        """
        爬取主贴时的总调度，获取总页数->循环获取单页内容content->循环处理content
        :return:
        """
        print("主帖：" + "第" + "1" + "页")
        content = self.CrawlMainTipContent(1)
        pageSymbols = re.findall(r'pagination:(.*),results', content)
        if len(pageSymbols):
            page_symbol = pageSymbols[0]
        else:
            description = "无法确定主贴总页数: " + self.courseUrl
            dict = {'logTime': datetime.now(), 'platform': '爱课程','description': description}
            logging.info(dict)
            raise Exception("无法确定主贴总页数1: " + self.courseUrl)
        self.dealMainPost(content)
        totalPage1 = re.search(page_symbol + r'\.totlePageCount=(.*);', content)
        totalPage = re.findall(page_symbol + r'\.totlePageCount=(.*);', totalPage1.group())
        if len(totalPage):
            if int(totalPage[0]) > 1:
                for i in range(2, int(totalPage[0]) + 1):
                    print("主贴：" + "第" + str(i) + "/" + totalPage[0] + "页")
                    content = self.CrawlMainTipContent(i)
                    self.dealMainPost(content)
        else:
            description = "无法确定主贴总页数: " + self.courseUrl
            dict = {'logTime': datetime.now(), 'platform': '爱课程（中国大学mooc）',
                    'description': description}
            logging.error(dict)
            raise Exception("无法确定主贴总页数2：" + self.courseUrl)
        print(len(self.mainTipList))
        pass

    def CrawlReplyTip(self):
        """
        爬取回复贴时的总调度，判断是否有回复帖->获取总页数->循环获取单页内容content->循环处理content
        :return:
        """
        for mainTip in self.mainTipList:
            self.CrawlMainTipIntro(mainTip)
            # print("主贴为：" + str(mainTip))
            if mainTip["replyCount"] != '0':
                mainTipId = mainTip["forum_post_id"]
                subject = mainTip["forum_subject"]
                forum_name = mainTip["forum_name"]
                print("回复帖：" + "第" + "1" + "页")
                content = self.CrawlReplyTipContent(mainTipId, 1)
                totalPage1 = re.findall(r'totalPageCount:\d+', content)
                # totalNum = re.findall(r'totalCount:\d+', content)[0].replace("totalCount:", '')
                self.dealReplyPost(forum_name, subject, mainTipId, content)
                if len(totalPage1):
                    totalPage = totalPage1[0].replace("totalPageCount:", '').replace(',', '')
                    if int(totalPage) > 1:
                        for i in range(2, int(totalPage) + 1):
                            print("回复帖：" + "第" + str(i) + "/" + totalPage + "页")
                            content = self.CrawlReplyTipContent(mainTipId, i)
                            self.dealReplyPost(forum_name, subject, mainTipId, content)
                else:
                    description = "爬取回复帖总页数时出现错误：下标位置为" + str(self.mainTipList.index(mainTip)) + "主贴标题：" + mainTip[
                        "title"]
                    dict = {'logTime': datetime.now(), 'platform': '爱课程（中国大学mooc）',
                            'description': description}
                    logging.error(dict)
                    raise Exception(description)
        pass

    def CrawlCommentTip(self):
        """
        爬取评论帖时的总调度（第三级爬取）
        :return:
        """
        for replyTip in self.replyTipList:
            if replyTip["replyCount"] != '0':
                replyTipId = replyTip["forum_post_id"]
                subject = replyTip["forum_subject"]
                forum_name = replyTip["forum_name"]
                print("评论帖：" + "第" + "1" + "页")
                content = self.CrawlCommentTipContent(replyTipId, 1)
                self.dealCommentPost(forum_name, subject, replyTipId, content)
                totalPageCount1 = re.search(r'totalPageCount:\d+', content)
                if totalPageCount1 == None:
                    description = "评论页数爬取失败, replyTipInfo:" + replyTip + "继续爬取"
                    dict = {'logTime': datetime.now(), 'platform': '爱课程（中国大学mooc）', 'status': 2,
                            'description': description}
                    logging.error(dict)
                    logging.info(description)
                    continue
                else:
                    totalPageCount = totalPageCount1.group().replace("totalPageCount:", '')
                if int(totalPageCount) > 1:
                    for i in range(2, int(totalPageCount) + 1):
                        content = self.CrawlCommentTipContent(replyTipId, i)
                        self.dealCommentPost(forum_name, subject, replyTipId, content)
        pass

    @retry(stop_max_attempt_number=5)
    def CrawlMainTipContent(self, i):
        """
        发起请求获取所有主贴的内容
        :param i: 页数
        :return: content 解码后的主贴内容
        """
        url = "http://www.icourse163.org/dwr/call/plaincall/PostBean.getAllPostsPagination.dwr"
        body = """callCount=1
scriptSessionId=${scriptSessionId}190
httpSessionId=1
c0-scriptName=PostBean
c0-methodName=getAllPostsPagination
c0-id=0
c0-param0=number:1
c0-param1=string:
c0-param2=number:1
c0-param3=number:1
c0-param4=number:20
c0-param5=boolean:false
c0-param6=null:null
batchId=1583999501285"""
        courseIdParam = 'c0-param0=number:' + self.tid
        pageParam = 'c0-param3=number:' + str(i)
        sessionPattern = 'httpSessionId=' + self.httpSessionId
        body = re.sub(r'httpSessionId=[0-9a-z]+', sessionPattern, body)
        body = re.sub(r'c0-param0=number:\d+', courseIdParam, body)
        body = re.sub(r'c0-param3=number:\d+', pageParam, body)
        session1 = requests.session()
        r1 = session1.post(url, data=body)
        time.sleep(0.1)
        content = r1.content.decode('unicode_escape')
        r1.close()
        session1.close()
        return content
        pass

    @retry(stop_max_attempt_number=5)
    def CrawlReplyTipContent(self, mainTipId, i):
        """
        发起请求获取回复帖的内容
        :param mainTipId: 主贴id,请求参数之一
        :param i: 页数
        :return:
        """
        url = "http://www.icourse163.org/dwr/call/plaincall/PostBean.getPaginationReplys.dwr"
        body = """callCount=1
scriptSessionId=${scriptSessionId}190
httpSessionId=8ed862d90f964e2abebbf2f0a5dff252
c0-scriptName=PostBean
c0-methodName=getPaginationReplys
c0-id=0
c0-param0=number:1216239824
c0-param1=number:2
c0-param2=number:1
batchId=1584102598604"""
        mainTipIdParam = 'c0-param0=number:' + mainTipId
        pageParam = 'c0-param2=number:' + str(i)
        sessionPattern = 'httpSessionId=' + self.httpSessionId
        body = re.sub(r'httpSessionId=[0-9a-z]+', sessionPattern, body)
        body = re.sub(r'c0-param0=number:\d+', mainTipIdParam, body)
        body = re.sub(r'c0-param2=number:\d+', pageParam, body)
        session2 = requests.session()
        r2 = session2.post(url, data=body)
        time.sleep(0.1)
        content = r2.content.decode('unicode_escape')
        r2.close()
        session2.close()
        return content
        pass

    @retry(stop_max_attempt_number=5)
    def CrawlCommentTipContent(self, replyId, i):
        """
        发起请求获得评论贴内容
        :param replyTip: 用于请求的参数
        :param i: 页数
        :return:
        """
        url = "http://www.icourse163.org/dwr/call/plaincall/PostBean.getPaginationComments.dwr"
        body = """callCount=1
scriptSessionId=${scriptSessionId}190
httpSessionId=e7df715c2b7a4e5583c0aa96b85df414
c0-scriptName=PostBean
c0-methodName=getPaginationComments
c0-id=0
c0-param0=number:1357978456
c0-param1=number:1
batchId=1584155267518"""
        reply_id_param = 'c0-param0=number:' + replyId
        page_pattern = 'c0-param1=number:' + str(i)
        session_pattern = 'httpSessionId=' + self.httpSessionId
        body = re.sub(r'httpSessionId=[0-9a-z]+', session_pattern, body)
        body = re.sub(r'c0-param0=number:\d+', reply_id_param, body)
        body = re.sub(r'c0-param1=number:\d+', page_pattern, body)
        session4 = requests.session()
        r4 = session4.post(url, data=body)
        time.sleep(0.1)
        content = r4.content.decode('unicode_escape')
        return content
        pass

    @retry(stop_max_attempt_number=5)
    def CrawlMainTipIntro(self, mainTip):
        """
        获取主贴introduction
        :param mainTip:
        :return:
        """
        url = "http://www.icourse163.org/dwr/call/plaincall/PostBean.getPostDetailById.dwr"
        body = """callCount=1
scriptSessionId=${scriptSessionId}190
httpSessionId=8ed862d90f964e2abebbf2f0a5dff252
c0-scriptName=PostBean
c0-methodName=getPostDetailById
c0-id=0
c0-param0=number:1216239824
batchId=1584102598201"""
        mainTipIdParam = 'c0-param0=number:' + mainTip["forum_post_id"]
        sessionPattern = 'httpSessionId=' + self.httpSessionId
        body = re.sub(r'httpSessionId=[0-9a-z]+', sessionPattern, body)
        body = re.sub(r'c0-param0=number:\d+', mainTipIdParam, body)
        session3 = requests.session()
        r3 = session3.post(url, data=body)
        time.sleep(0.1)
        content = str(r3.content.decode('unicode_escape'))
        # pre = re.compile('>(.*?)<')
        # if len(pre.findall(content, re.S)):
        #     mainTip["shortIntroduction"] = ''.join(pre.findall(content, re.S))
        # else:
        #     mainTip["shortIntroduction"] = re.search(r'content:"(.*)",', content, re.S).group().split('"')[1]
        # mainTip["shortIntroduction"] = re.sub(r'\n|&nbsp|\xa0|&gt|&lt|\t|\'D\'', '', mainTip["shortIntroduction"])
        # mainTip["shortIntroduction"] = self.dealCharacter(mainTip["shortIntroduction"])
        title_text = re.search(r'content:".*(",count)', content, re.S)
        if title_text == None:
            mainTip["forum_post_content"] = re.search(r'content:"(.*)",', content, re.S).group().split('"')[1]
        else:
            mainTip["forum_post_content"] = title_text.group().replace(",count", '').replace('content:"', '')
        html = HTML(html=mainTip["forum_post_content"])
        mainTip["forum_post_content"] = html.text
        mainTip["forum_post_content"] = re.sub(r'\n|&nbsp|\xa0|&gt|&lt|\t|\'D\'', '', mainTip["forum_post_content"])
        mainTip["forum_post_content"] = self.dealCharacter(mainTip["forum_post_content"])
        r3.close()
        session3.close()

    pass

    def get_forum_name(self, forum_post_id):
        url = "http://www.icourse163.org/dwr/call/plaincall/MocForumBean.getForumNavi.dwr"
        body = """callCount=1
scriptSessionId=${scriptSessionId}190
httpSessionId=0fc83d4f8dbf4559bab3ba4bd28d33ff
c0-scriptName=MocForumBean
c0-methodName=getForumNavi
c0-id=0
c0-param0=null:null
c0-param1=number:1312498308
c0-param2=boolean:false
c0-param3=number:1450219455
batchId=1585632353340"""
        post_Param = 'c0-param1=number:' + forum_post_id
        sessionPattern = 'httpSessionId=' + self.httpSessionId
        term_param = 'c0-param3=number:' + self.tid
        body = re.sub(r'httpSessionId=[0-9a-z]+', sessionPattern, body)
        body = re.sub(r'c0-param1=number:\d+', post_Param, body)
        body = re.sub(r'c0-param3=number:\d+', term_param, body)
        session = requests.session()
        res = session.post(url, data=body)
        time.sleep(0.1)
        content = res.content.decode('unicode_escape')
        forum = re.search(r'forumName:"(.*)",', content).group()
        forum_name = re.findall(u'[\u4E00-\u9FFF]+', forum)
        forum_name = ''.join(forum_name)
        if forum_name == '':
            forum_name = "无法识别"
        return forum_name
        pass

    def dealCharacter(self, str):
        result = re.findall(u'[\u4E00-\u9FFF0-9A-Za-z，,。；.%!！#：:?？《》<>$^{}()（）+=;\-*/]+', str, re.S)
        str = ''
        for item in result:
            str += item
        str = re.sub('<imgsrc=', '', str, re.S)
        str = re.sub('<codeclass=', '', str, re.S)
        str = re.sub('<ahref=', '', str, re.S)
        str = re.sub('quot', '', str, re.S)
        return str
        pass

    def run_by_url_forum(self, course_url: str, login_info: dict) -> dict:
        logging.basicConfig(filename="icourse.log", level=logging.INFO)
        self.courseUrl = course_url
        self.tid = re.findall(r'tid=[0-9a-z]+', self.courseUrl)[0].replace("tid=", "")
        self.CrawlMainTip()
        self.CrawlReplyTip()
        self.CrawlCommentTip()
        for item1 in self.mainTipList:
            item1.pop("replyCount")
        for item2 in self.replyTipList:
            item2.pop("replyCount")
        total_list = self.mainTipList + self.replyTipList + self.commentTipList
        dict = {
            "forum_post_info": total_list,
            "error_list": self.error_list
        }
        return dict
        pass


if __name__ == '__main__':
    test = ForumFetcher()
    total_list = test.run_by_url_forum("http://www.icourse163.org/course/NEU-1003531008?tid=1207030227", {})
    # 把course换成learn ,然后在后面加上#/learn/forumindex, 可以直接进入课程详情页