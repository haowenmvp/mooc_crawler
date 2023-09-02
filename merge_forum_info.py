import time

import mysql.connector
from persistence.db.forum_basic_info_repo import ForumBasicInfoRepository
from persistence.db.forum_post_info_repo import ForumPostInfoRepository
from persistence.db.forum_post_info_temporary_repo import ForumPostInfoTemporaryRepository
from persistence.model.forum import ForumPostInfo


class MergeForumInfo(object):
    def __init__(self, config):
        self.forum_post_info_repo = ForumPostInfoRepository(**config)
        self.forum_post_info_temporary_repo = ForumPostInfoTemporaryRepository(**config)
        self.forum_basic_info_repo = ForumBasicInfoRepository(**config)
        self.__conn = mysql.connector.connect(host=config["host"], user=config["username"], password=config["password"],
                                              database=config["database"], port=config["port"])
        pass

    def connect(self):
        self.__conn = mysql.connector.connect(host=config["host"], user=config["username"], password=config["password"],
                                              database=config["database"], port=config["port"])

    def merge_forum_info(self):
        all_key_info = self.forum_post_info_temporary_repo.getAllKeyInfo()
        update_time = all_key_info[0][5]
        for per_forum in all_key_info:
            is_exist = self.forum_post_info_repo.query_is_exist_by_part_info(per_forum)
            if is_exist:
                tuple_info = self.forum_post_info_temporary_repo.get_detail_info_by_id(per_forum[0])
                forum_post_info = self.__construct_forum_post_info_by_tuple(tuple_info)
                self.forum_post_info_repo.update_forum_post_info(forum_post_info, is_exist)
            else:
                tuple_info = self.forum_post_info_temporary_repo.get_detail_info_by_id(per_forum[0])
                forum_post_info = self.__construct_forum_post_info_by_tuple(tuple_info)
                self.forum_post_info_repo.create_forum_post_info(forum_post_info)
        print("-----data is transferred to forum_post_info table-----")
        forum_ids = self.get_distinct_forum_id()
        for forum_id in forum_ids:
            semester_forum_ids = self.forum_basic_info_repo.query_all_forum_ids_by_forum_id(forum_id)
            for item in semester_forum_ids:
                self.forum_post_info_repo.if_update_time_not_today_invalid(item, update_time)
        print("-----forum_post_info table isvalid field has been updated-----")
        self.clear_forum_post_info_temporary()
        print("-----forum_post_info_temporary table has been cleared-----")
        pass

    def clear_forum_post_info_temporary(self):
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        sql = "SELECT COUNT(id) FROM forum_post_info_temporary"
        cursor.execute(sql)
        results = cursor.fetchall()
        counts = results[0][0]
        for item in range(0, int(counts/1000) + 1):
            sql = "DELETE FROM forum_post_info_temporary LIMIT 1000"
            cursor.execute(sql)
            print(sql)
            self.__conn.commit()
            time.sleep(0.5)
        pass

    def get_distinct_forum_id(self):
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        sql = "SELECT DISTINCT(forum_id) FROM forum_post_info_temporary"
        cursor.execute(sql)
        results = cursor.fetchall()
        forum_ids = list()
        for item in results:
            forum_ids.append(item[0])
        return forum_ids

    def __construct_forum_post_info_by_tuple(self, tuple_info: tuple) -> ForumPostInfo:
        forum_post_info = ForumPostInfo()
        forum_post_info.platform = tuple_info[1]
        forum_post_info.forum_post_id = tuple_info[2]
        forum_post_info.forum_id = tuple_info[3]
        forum_post_info.forum_name = tuple_info[4]
        forum_post_info.forum_subject = tuple_info[5]
        forum_post_info.forum_post_type = tuple_info[6]
        forum_post_info.forum_reply_id = tuple_info[7]
        forum_post_info.forum_reply_userid = tuple_info[8]
        forum_post_info.forum_post_username = tuple_info[9]
        forum_post_info.forum_post_userrole = tuple_info[10]
        forum_post_info.forum_post_content = tuple_info[11]
        forum_post_info.forum_post_time = tuple_info[12]
        forum_post_info.update_time = tuple_info[13]
        forum_post_info.isvalid = tuple_info[14]
        return forum_post_info


if __name__ == '__main__':
    config = {"host": "localhost", "port": 3306, "username": "root", "password": "943652865",
              "database": "mooc_test"}
    test = MergeForumInfo(config)
    test.merge_forum_info()
