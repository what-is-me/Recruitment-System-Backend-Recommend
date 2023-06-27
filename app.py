import json
import random
from math import sqrt
from time import time

import pymongo
from fastapi import FastAPI
from pyttl import TTLDict

"""
基于用户的协同过滤算法
"""


def mongodb():
    return pymongo.MongoClient(
        host='10.100.164.30',
        port=27017,
        username='root',
        password='root',
        authSource='admin',
        authMechanism='SCRAM-SHA-256'
    )['employee']


def calc_similarity(obj1: dict, obj2: dict) -> float:
    """
    :param obj1: user1
    :type obj1: dict
    :param obj2: user2
    :type obj2: dict
    :return: calc similarity between user:obj1 and user:obj2
    :rtype: float
    """
    score1, score2 = obj1.get('score', {}), obj2.get('score', {})
    if len(score1) > len(score2):
        score1, score2 = score2, score1
    AB = 0.0  # sum(A[i]*B[i])
    A2 = sum(map(lambda x: x ** 2, score1.values()))  # A = sqrt(sum(A[i]^2))
    B2 = sum(map(lambda x: x ** 2, score2.values()))  # B = sqrt(sum([i]^2))
    for key in score1.keys():
        AB += score1.get(key, 0.0) * score2.get(key, 0.0)
    if AB == 0 or A2 == 0 or B2 == 0:
        return 0
    return AB / sqrt(A2 * B2)


def normalization(score_dict: dict) -> dict:
    """
    :param score_dict: 评价向量
    :type score_dict: dict
    :return: 归一化的评价向量
    :rtype: dict
    """
    max_value = max(score_dict.values())
    if max_value == 0:
        return score_dict
    max_value = max_value ** 0.75
    return {
        key: value ** 0.75 / max_value
        for key, value in score_dict.items()
    }


class User:
    def __init__(self, uid: str, max_k: int = 5, max_n: int = 5, limit_time: int = 60):

        self.uid = uid
        self.users = {}
        self.job_keys = []
        self.jobs = {}
        self.max_k = max_k
        self.max_n = max_n
        self.limit_time = limit_time

        self.timespan = 0
        self.similarity_vector = []
        self.job_score_vector = []
        self.jobs_recommend = []

    def upgrade_value(self):
        db = mongodb()
        self.jobs = {item['_id']: item for item in db['job'].find()}
        self.job_keys = list(self.jobs.keys())
        random.shuffle(self.job_keys)
        self.users = {
            item['_id']: {
                '_id': item['_id'],
                'score': normalization(item['score'])
            }
            for item in db['recommend'].find()
        }
        if self.uid not in self.users:
            self.users[self.uid] = {'_id': self.uid, 'score': {}}

        self.calc_k_similar_user()
        self.calc_n_highest_score_job()
        self.find_all_jobs_by_id()
        self.timespan = time()

    def calc_k_similar_user(self):
        thisUser = self.users[self.uid]
        self.similarity_vector = list(filter(
            lambda x: x[1] > 0.3,
            [(user_info['_id'], calc_similarity(thisUser, user_info))
             for user_info in self.users.values()]
        ))
        self.similarity_vector.sort(key=lambda item: -item[1])
        if len(self.similarity_vector) > self.max_k:
            self.similarity_vector = self.similarity_vector[:self.max_k]

    def calc_n_highest_score_job(self):
        k = len(self.similarity_vector)
        users_score_dict = [self.users[item[0]]['score'] for item in self.similarity_vector]
        job_keys = self.job_keys
        self.job_score_vector = []
        for job_key in job_keys:
            job_score = 0.0
            for score_dict in users_score_dict:
                job_score += score_dict.get(job_key, 0.0)
            if k != 0:
                job_score /= k
            self.job_score_vector.append((job_key, job_score))
        self.job_score_vector.sort(key=lambda item: -item[1])
        if len(self.job_score_vector) > self.max_n:
            self.job_score_vector = self.job_score_vector[:self.max_n]

    def find_all_jobs_by_id(self):
        self.jobs_recommend = []
        for key, _ in self.job_score_vector:
            job = self.jobs[key]
            self.jobs_recommend.append(job)

    def get_recommend_lazy(self):
        if time() - self.timespan > self.limit_time:
            self.upgrade_value()
        return {
            'update_time': int(self.timespan * 1000),
            'similarity': self.similarity_vector,
            'job_score': self.job_score_vector,
            'jobs': self.jobs_recommend
        }


def pretty_print(dic):
    js = json.dumps(dic, sort_keys=True, indent=4, separators=(',', ':'))
    print(js)


app = FastAPI()
user_dict = TTLDict()


@app.get("/recommend/{uid}")
async def say_hello(uid: str):
    if uid not in user_dict:
        user_dict.setex(uid, 60 * 60, User(uid, limit_time=60))
    user = user_dict[uid]
    return user.get_recommend_lazy()
