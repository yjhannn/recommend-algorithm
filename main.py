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
    def run_algorithm(self, user_id, category_id):
        # 해당 카테고리의 모든 영상을 가져옴
        video_keys = self.redis.keys(f"video_{category_id}:*")

        for video_key in video_keys:
            video_id = video_key.decode().split(":")[1]  # video_id 추출

            # 영상 정보 불러오기
            view_count = int(self.redis.hget(video_key, "view_count") or 0)
            like_count = int(self.redis.hget(video_key, "like_count") or 0)
            upload_date = self.redis.hget(video_key, "created_at")

            # 사용자와 영상에 대한 상호작용 정보 불러오기
            interaction_key = f"interaction:{user_id}:{video_id}"
            watched = int(self.redis.hget(interaction_key, "watched") or 0)
            liked = int(self.redis.hget(interaction_key, "liked") or 0)

            # 가중치 설정
            view_weight = 0.004  # 조회수 가중치
            like_weight = 0.04  # 좋아요 가중치
            liked_weight = 2.0  # 사용자가 좋아요를 눌렀을 때의 가중치
            watch_penalty = 1.0  # 사용자가 시청한 경우 감점
            recency_weight = 0.02  # 최신성 가중치

            # 최신성 계산
            now = datetime.now()
            video_date = datetime.fromisoformat(upload_date.decode())
            days_diff = (now - video_date).days

            # 점수 계산
            popularity_score = view_count * view_weight + like_count * like_weight
            user_score = (liked_weight if liked else 0) - (watch_penalty if watched else 0)
            recency_score = max(0, recency_weight * (30 - days_diff))  # 최근 30일 내의 영상은 높은 점수
            total_score = popularity_score + user_score + recency_score

            # 점수를 Redis에 저장
            score_key = f"scores:{user_id}"
            self.redis.zadd(score_key, {f"{category_id}:{video_id}": total_score})

        # 마지막 업데이트 시간 갱신
        self.redis.hset(f"user_meta:{user_id}", "last_updated_at", datetime.now().isoformat())


    def getRequests(self, queue):
        return True

    # def update_scores(self, user_id, video_id):
    #     score = self.runAlgorithm(user_id, video_id)
    #     score_key = f"scores:{user_id}"
    #     self.redis.zadd(score_key, {video_id: score})
    #     self.redis.hset(f"user_meta:{user_id}", "last_updated_at", datetime.now().isoformat())

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