import redis
import json
from datetime import datetime
from eventQueue import EventQueue


class VideoRecommender:

    def __init__(self, redis_client):
        self.redis = redis_client
        self.event_queue = EventQueue(self.redis)

    def add_videoInfo(self, category_id, video_id, created_at):
        video_key = f"video_{category_id}:{video_id}"
        self.redis.hset(video_key, mapping={
            "created_at": created_at.isoformat(),
            "view_count": 0,
            "like_count": 0
        })

    # 유저의 행위로 이벤트 발생, 스케줄러에 추천 영상 리스트 계산 작업 요청 추가
    def get_new_event(self, user_id, category_id, video_id, watched=False, liked=False):
        reaction_key = f"reaction:{user_id}:{video_id}"
        video_key = f"video_{category_id}:{video_id}"

        if watched:
            self.redis.hset(reaction_key, "watched", 1)
            self.redis.hincrby(video_key, "view_count", 1)
        if liked == 1:
            self.redis.hset(reaction_key, "liked", 1)
            self.redis.hincrby(video_key, "like_count", 1)
        elif liked == -1:
            self.redis.hset(reaction_key, "liked", 0)
            # self.redis.hset(video_key, "like_count", -1)

        new_event = {
            "user_id": user_id,
            "category_id": category_id,
            "last_updated_at": self.redis.hget(f"user_meta:{user_id}", "last_updated_at")
        }

        self.event_queue.add_event(new_event)

    # 실제 계산이 수행되는 알고리즘
    def run_algorithm(self, user_id, category_id):
        # 해당 카테고리의 모든 영상을 가져옴
        video_keys = self.redis.keys(f"video_{category_id}:*")

        for video_key in video_keys:
            video_id = video_key.decode().split(":")[1]  # video_id 추출

            # 영상 정보 불러오기
            view_count = int(self.redis.hget(video_key, "view_count") or 0)
            like_count = int(self.redis.hget(video_key, "like_count") or 0)
            upload_date = self.redis.hget(video_key, "created_at")

            # 사용자와 영상에 대한 반응 정보 불러오기
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

        # 카테고리에 해당되는 영상들 모두 점수 부여 후 마지막 업데이트 시간 갱신
        self.redis.hset(f"user_meta:{user_id}", "last_updated_at", datetime.now().isoformat())


    def update_scores(self, user_id, category_id):
        score = self.run_algorithm(user_id, category_id)
        # score_key = f"scores:{user_id}"
        # self.redis.zadd(score_key, {video_id: score})
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