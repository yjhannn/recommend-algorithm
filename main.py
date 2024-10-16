import redis
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from fastapi import FastAPI, Response, Cookie


class RecommentAlgorithim:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    def __init__(self, host=os.environ["REDIS_HOST"], port=os.environ["REDIS_PORT"], db=os.environ["REDIS_DATABASE"]):
        self.redis = redis.Redis(host=host, port=port, db=db)

    def add_videoInfo(self, video_id, created_at):
        video_key = f"video:{video_id}"
        self.redis.hset(video_key, mapping={
            "created_at": created_at.isoformat(),
            "view_count": 0,
            "like_count": 0
        })

    def get_event(self, user_id, video_id, watched=False, liked=False):
        reaction_key = f"reaction:{user_id}:{video_id}"
        video_key = f"video:{video_id}"

        if watched:
            self.redis.hset(reaction_key, "watched", 1)
            self.redis.hincrby(video_key, "view_count", 1)
        if liked == 1:
            self.redis.hset(reaction_key, "liked", 1)
            self.redis.hincrby(video_key, "like_count", 1)
        elif liked == -1:
            self.redis.hset(reaction_key, "liked", 0)ㅅ
            # self.redis.hset(video_key, "like_count", -1)

    # 추후 구현이 필요한 메소드들
    def run_algorithm(self, user_id, video_id):
        video_key = f"video:{video_id}"
        view_count = int(self.redis.hget(video_key, "view_count") or 0)
        like_count = int(self.redis.hget(video_key, "like_count") or 0)
        upload_date = self.redis.hget(video_key, "created_at")

        interaction_key = f"interaction:{user_id}:{video_id}"
        watched = int(self.redis.hget(interaction_key, "watched") or 0)
        liked = int(self.redis.hget(interaction_key, "liked") or 0)

        # 가중치 설정
        view_weight = 0.004 # 조회수에 대한 가중치
        like_weight = 0.04 # 좋아요 수에 대한 가중치
        liked_weight = 2.0 # 유저의 좋아요 반응 여부에 대한 가중치
        watch_penalty = 1.0 # 유저의 시청 여부에 대한 감점 가중치(-0.5로 적용)
        recency_weight = 0.02 # 영상의 최신성에 대한 가중치

        # 최신성 계산(업로드 날짜와 현재 시간의 차이)
        now = datetime.now()
        video_date = datetime.fromisoformat(upload_date.decode())
        days_diff = (now - video_date).days

        # 점수 계산
        popularity_score = view_count * view_weight + like_count * like_weight # 인기도 점수
        user_score = (liked_weight if liked else 0) - (watch_penalty if watched else 0) # 시청 여부 및 반응 점수
        recency_score = max(0, recency_weight * (30 - days_diff))  # 최신성 점수(최근 30일 내 영상일수록 점수가 높음)
        total_score = popularity_score + user_score + recency_score

        return total_score

    def getRequests(self, queue):
        return True

    def update_scores(self, user_id, video_id):
        score = self.runAlgorithm(user_id, video_id)
        score_key = f"scores:{user_id}"
        self.redis.zadd(score_key, {video_id: score})
        self.redis.hset(f"user_meta:{user_id}", "last_updated_at", datetime.now().isoformat())

    # 계산된 추천 데이터를 받아온다.
    def get_recommendations(self, user_id, count=10):
        score_key = f"scores:{user_id}"
        recommendation_list = self.redis.zrevrange(score_key, 0, count - 1)  # 내림차순 정렬 반환
        last_updated_at = self.redis.hget(f"user_meta:{user_id}", "last_updated_at")

        result = {
            "user_id": user_id,
            "recommend_videos": [
                rec.decode() for rec in recommendation_list
            ],
            "last_updated_at": last_updated_at
        }

        return json.dumps(result)

    def close(self):
        self.redis.close()


# 조회 명령어: hgetall {key}
app = FastAPI()
recommender = RecommentAlgorithim()
recommender.add_videoInfo(1, datetime.now())
recommender.add_videoInfo(2, datetime.now())