# -*- coding:utf-8 -*-

import MySQLdb
import pymongo
import json
from py2neo import Graph,Node,Relationship
from neo4j.v1 import GraphDatabase
import logging
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

class Citation2Mongo():

    def __init__(self):
        self.db = MySQLdb.connect(host="localhost", user="root",passwd="stx11stx11", db="dblp_ref")
        self.client = pymongo.MongoClient('localhost',27017)
        self.mongdb = self.client.Paper
        self.my_set = self.mongdb.Co_authors
    def read(self, file_path):
        items = []
        count = 0
        json_file = open(file_path, "r")
        for i, line in enumerate(json_file):

            if i % 1000 == 0:
            # if i==5:
                print i
                # print "关系数： ",count
                self.insert_item(items)
                items = []


            json_line = json.loads(line)
            co_authors = json_line["authors"] if  "authors" in json_line else ""
            # references = json_line["references"] if "references" in json_line else ""
            id = json_line["id"] if "id" in json_line else ""
            item = {"paper_id":id,"co_authors":co_authors}

                # for ref_id_source in references:
                #     ref_id = ref_id_source.encode('ascii')
                #     item = (id, ref_id)
                #     items.append(item)
            count += 1
            items.append(item)
                # print line
        self.insert_item(items)
        json_file.close()
        print "总数： ",count
        print "done!!"

    def insert_item(self, items):
        try:
            self.my_set.insert(items)

        except Exception as e:
            logging.error(e)

if __name__=="__main__":
    dmongo = Citation2Mongo()
    # file = "../data2mysql/dblp-ref/test.json"
    # dmongo.read(file)
    file_path_0 = "../data2mysql/dblp-ref/dblp-ref-0.json"
    file_path_1 = "../data2mysql/dblp-ref/dblp-ref-1.json"
    file_path_2 = "../data2mysql/dblp-ref/dblp-ref-2.json"
    file_path_3 = "../data2mysql/dblp-ref/dblp-ref-3.json"
    dmongo.read(file_path_0)
    dmongo.read(file_path_1)
    dmongo.read(file_path_2)
    dmongo.read(file_path_3)


