import os
import asyncio
import schedule
import time
import json
import logging
import aiohttp
from datetime import datetime
import pytz
from googleapiclient.discovery import build
from dotenv import load_dotenv
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_trends.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv('.env.local')

# 디버그 모드 설정
DEBUG_MODE = True

def debug_print(message):
    """디버그 메시지 출력"""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

# API 키 설정
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, YOUTUBE_API_KEY]):
    raise ValueError("필요한 환경 변수가 설정되지 않았습니다. .env.local 파일을 확인해주세요.")

def get_korea_time():
    """한국 시간을 반환하는 함수"""
    korea_tz = pytz.timezone('Asia/Seoul')
    return datetime.now(korea_tz)

def is_update_time() -> bool:
    """현재 시간이 업데이트 가능한 시간인지 확인"""
    if DEBUG_MODE:
        return True
        
    current_time = get_korea_time()
    hour = current_time.hour
    
    # 밤 1시부터 6시 사이에는 업데이트하지 않음
    # if 1 <= hour < 6:
    #     logger.info(f"현재 시간 {hour}시는 업데이트 시간이 아닙니다. (업데이트 시간: 6시 ~ 1시)")
    #     return False
    return True

def is_daily_update_time() -> bool:
    """현재 시간이 일일 전체 업데이트 시간(6시)인지 확인"""
    # 테스트 모드에서는 6시로 가정하지 않음
    if DEBUG_MODE:
        return False
        
    current_time = get_korea_time()
    return current_time.hour == 6 and current_time.minute < 5  # 6시 0분~5분 사이에만 실행

def is_daily_summary_time() -> bool:
    """현재 시간이 일일 요약 시간(1시)인지 확인"""
    if DEBUG_MODE:
        return False
        
    current_time = get_korea_time()
    return current_time.hour == 1

@dataclass
class TrendItem:
    """트렌드 아이템 데이터 클래스"""
    title: str
    rank: int
    channel: str = ""
    views: str = ""
    url: str = ""

class YouTubeTrendsBot:
    def __init__(self):
        self.youtube = None
        self.retry_count = 3
        self.retry_delay = 5
        self.data_dir = Path("youtube_trends_data")
        self.data_dir.mkdir(exist_ok=True)
        self.is_first_run = self._load_first_run_state()
        self.sent_urls = set()  # 전송된 URL을 추적하는 세트
        self._load_sent_urls()  # 초기화시 URL 로드
        self.last_sent_time = {}  # 마지막 전송 시간을 추적하는 딕셔너리

    def _get_data_file_path(self, country: str) -> Path:
        """데이터 파일 경로 반환"""
        return self.data_dir / f"youtube_{country}.json"

    def _get_first_run_file_path(self) -> Path:
        """첫 실행 상태 파일 경로 반환"""
        return self.data_dir / "first_run.json"

    def _get_sent_urls_file_path(self) -> Path:
        """전송된 URL 파일 경로 반환"""
        return self.data_dir / "sent_urls.json"

    def _get_last_sent_time_file_path(self) -> Path:
        """마지막 전송 시간 파일 경로 반환"""
        return self.data_dir / "last_sent_time.json"

    def _load_first_run_state(self) -> bool:
        """첫 실행 상태 로드"""
        file_path = self._get_first_run_file_path()
        if not file_path.exists():
            return True
        
        try:
            with open(file_path, 'r') as f:
                return json.load(f)['is_first_run']
        except:
            return True

    def _save_first_run_state(self):
        """첫 실행 상태 저장"""
        file_path = self._get_first_run_file_path()
        with open(file_path, 'w') as f:
            json.dump({'is_first_run': False}, f)
        logger.info("첫 실행 상태를 false로 저장했습니다.")

    def _save_trends_data(self, country: str, data: List[TrendItem]):
        """트렌드 데이터 저장"""
        file_path = self._get_data_file_path(country)
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([item.__dict__ for item in data], f, ensure_ascii=False, indent=2)
            logger.info(f"{country} 트렌드 데이터 저장 완료")
        except Exception as e:
            logger.error(f"{country} 트렌드 데이터 저장 실패: {str(e)}")

    def _load_trends_data(self, country: str) -> List[TrendItem]:
        """트렌드 데이터 로드"""
        file_path = self._get_data_file_path(country)
        try:
            if not file_path.exists():
                logger.info(f"{country} 트렌드 데이터 파일이 없습니다.")
                return []
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"{country} 트렌드 데이터 로드 완료")
                return [TrendItem(**item) for item in data]
        except Exception as e:
            logger.error(f"{country} 트렌드 데이터 로드 실패: {str(e)}")
            return []

    def _load_sent_urls(self):
        """전송된 URL 로드"""
        file_path = self._get_sent_urls_file_path()
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    self.sent_urls = set(json.load(f))
                logger.info(f"전송된 URL {len(self.sent_urls)}개를 로드했습니다.")
            except Exception as e:
                logger.error(f"전송된 URL 로드 실패: {str(e)}")
                self.sent_urls = set()
        else:
            logger.info("전송된 URL 파일이 없습니다. 새로운 세트를 생성합니다.")
            self.sent_urls = set()

    def _save_sent_urls(self):
        """전송된 URL 저장"""
        file_path = self._get_sent_urls_file_path()
        try:
            with open(file_path, 'w') as f:
                json.dump(list(self.sent_urls), f)
            logger.info(f"전송된 URL {len(self.sent_urls)}개를 저장했습니다.")
        except Exception as e:
            logger.error(f"전송된 URL 저장 실패: {str(e)}")

    def _reset_sent_urls(self):
        """전송된 URL 초기화 (매일 6시에 호출)"""
        self.sent_urls.clear()
        self._save_sent_urls()
        logger.info("전송된 URL을 초기화했습니다.")

    def _load_last_sent_time(self):
        """마지막 전송 시간 로드"""
        file_path = self._get_last_sent_time_file_path()
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    self.last_sent_time = json.load(f)
                logger.info("마지막 전송 시간을 로드했습니다.")
            except:
                self.last_sent_time = {}
        else:
            self.last_sent_time = {}

    def _save_last_sent_time(self):
        """마지막 전송 시간 저장"""
        file_path = self._get_last_sent_time_file_path()
        try:
            with open(file_path, 'w') as f:
                json.dump(self.last_sent_time, f)
            logger.info("마지막 전송 시간을 저장했습니다.")
        except Exception as e:
            logger.error(f"마지막 전송 시간 저장 실패: {str(e)}")

    def _detect_changes(self, old_data: List[TrendItem], new_data: List[TrendItem]) -> Dict[str, List[Any]]:
        """트렌드 변경 감지"""
        changes = {
            'new': [],
            'up': [],    # 순위 상승
            'down': [],  # 순위 하락
            'same': []   # 순위 동일
        }

        # 이전 데이터를 딕셔너리로 변환 (key: video_id, value: 순위와 제목)
        old_items = {}
        for item in old_data:
            video_id = item.url.split("/")[-1]
            old_items[video_id] = {"rank": item.rank, "title": item.title}
        
        logger.info(f"이전 데이터: {len(old_items)}개")
        
        # 새로운 항목과 순위 변경 감지
        for new_item in new_data:
            video_id = new_item.url.split("/")[-1]
            
            if video_id in old_items:
                old_rank = old_items[video_id]["rank"]
                if new_item.rank < old_rank:  # 순위 상승
                    logger.info(f"순위 상승: {new_item.title} ({old_rank}위 → {new_item.rank}위)")
                    changes['up'].append({
                        'item': new_item,
                        'old_rank': old_rank,
                        'new_rank': new_item.rank
                    })
                elif new_item.rank > old_rank:  # 순위 하락
                    logger.info(f"순위 하락: {new_item.title} ({old_rank}위 → {new_item.rank}위)")
                    changes['down'].append({
                        'item': new_item,
                        'old_rank': old_rank,
                        'new_rank': new_item.rank
                    })
                else:  # 순위 동일
                    changes['same'].append(new_item)
            else:
                # 완전히 새로운 항목
                logger.info(f"신규 진입: {new_item.title} ({new_item.rank}위)")
                changes['new'].append(new_item)

        # 변경사항 로깅
        logger.info(f"변경사항 요약:")
        logger.info(f"- 신규 진입: {len(changes['new'])}개")
        logger.info(f"- 순위 상승: {len(changes['up'])}개")
        logger.info(f"- 순위 하락: {len(changes['down'])}개")
        logger.info(f"- 순위 유지: {len(changes['same'])}개")

        return changes

    def _format_full_trends_message(self, country: str, data: List[TrendItem]) -> str:
        """전체 트렌드 데이터 메시지 포맷팅"""
        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        current_time = get_korea_time()
        
        # 요일 한글 변환
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"📌 {country_emoji} {country_name} 유튜브 트렌드 ({date_str})\n"
        #message += "📊 매일 오전 6시 최신 순위 업데이트\n\n"
        
        # 순위 이모지 매핑
        rank_emojis = {
            1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣",
            6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
        }
        
        for item in data:
            rank_emoji = rank_emojis.get(item.rank, f"{item.rank}위")
            message += f"{rank_emoji} [{item.title}]\n"
            message += f"📺 {item.channel} | 👁️ {item.views}회\n"
            message += f"🔗 {item.url}\n\n"
        
        return message

    def _format_new_items_message(self, new_items: List[TrendItem], country: str) -> str:
        """새로운 항목 메시지 포맷팅"""
        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        current_time = get_korea_time()
        
        # 요일 한글 변환
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"📌 {country_emoji} {country_name} 유튜브 트렌드 신규 진입 ({date_str})\n"
        message += "📊 새로운 인기 동영상이 등장했습니다\n\n"
        
        # 순위 이모지 매핑
        rank_emojis = {
            1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣",
            6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
        }
        
        for item in new_items:
            rank_emoji = rank_emojis.get(item.rank, f"{item.rank}위")
            message += f"{rank_emoji} [{item.title}]\n"
            message += f"📺 {item.channel} | 👁️ {item.views}회\n"
            message += f"🔗 {item.url}\n\n"
        
        return message

    def _format_night_mode_message(self) -> str:
        """야간 모드 메시지 포맷팅"""
        # return "※ 새벽 2시~6시는 알림이 없습니다. 편안한 밤 되세요 🌙"
        return "편안한 밤 되세요 🌙"

    def _format_changes_message(self, changes: Dict[str, List[Any]], country: str) -> Optional[str]:
        """변경사항 메시지 포맷팅"""
        # 변경사항이 없으면 None 반환
        if not (changes['new'] or changes['up'] or changes['down']):
            logger.info("변경사항이 없어 메시지를 전송하지 않습니다.")
            return None

        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        current_time = get_korea_time()
        
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"📌 {country_emoji} {country_name} 유튜브 트렌드 업데이트 ({date_str})\n"
        message += "📊 순위 변경 및 신규 진입 동영상\n\n"
        
        # 순위 이모지 매핑
        rank_emojis = {
            1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣",
            6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
        }
        
        # 모든 변경사항 합치기
        all_changes = []
        
        # 신규 진입
        for item in changes['new']:
            all_changes.append({
                'item': item,
                'type': 'new',
                'rank': item.rank
            })
        
        # 순위 상승
        for change in changes['up']:
            all_changes.append({
                'item': change['item'],
                'type': 'up',
                'old_rank': change['old_rank'],
                'new_rank': change['new_rank']
            })

        # 순위 하락
        for change in changes['down']:
            all_changes.append({
                'item': change['item'],
                'type': 'down',
                'old_rank': change['old_rank'],
                'new_rank': change['new_rank']
            })
        
        # 순위로 정렬
        all_changes.sort(key=lambda x: x['item'].rank)
        
        # 변경사항 포맷팅
        for change in all_changes:
            item = change['item']
            rank_emoji = rank_emojis.get(item.rank, f"{item.rank}위")
            
            if change['type'] == 'new':
                message += f"{rank_emoji} [{item.title}] New\n"
            elif change['type'] == 'up':
                message += f"{rank_emoji} [{item.title}] {change['old_rank']} → {change['new_rank']}\n"
            elif change['type'] == 'down':
                message += f"{rank_emoji} [{item.title}] {change['old_rank']} → {change['new_rank']}\n"
            
            message += f"📺 {item.channel} | 👁️ {item.views}회\n"
            message += f"🔗 {item.url}\n\n"
        
        return message

    def _format_daily_summary(self, country: str) -> str:
        """일일 요약 메시지 포맷팅"""
        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        current_time = get_korea_time()
        
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"📋 {country_emoji} {country_name} 유튜브 트렌드 일일 요약 ({date_str})\n\n"
        
        try:
            data = self._load_trends_data(country)
            if not data:
                return message + "데이터가 없습니다."

            rank_emojis = {
                1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣",
                6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
            }

            for item in data:
                rank_emoji = rank_emojis.get(item.rank, f"{item.rank}위")
                message += f"{rank_emoji} [{item.title}]\n"
                message += f"📺 {item.channel} | 👁️ {item.views}회\n"
                message += f"🔗 {item.url}\n\n"

            message += "\n🌙 오늘 하루도 수고하셨습니다. 편안한 밤 되세요."
            return message
        except Exception as e:
            logger.error(f"일일 요약 생성 중 에러 발생: {str(e)}")
            return message + "요약 생성 중 에러가 발생했습니다."

    def init_youtube(self):
        """YouTube API 클라이언트 초기화"""
        if not self.youtube:
            self.youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

    async def send_telegram_message(self, message: str) -> bool:
        """텔레그램으로 메시지를 전송하는 함수"""
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            data = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            }
            
            for attempt in range(self.retry_count):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, json=data) as response:
                            if response.status == 200:
                                logger.info("텔레그램 메시지 전송 성공")
                                return True
                            else:
                                logger.error(f"텔레그램 메시지 전송 실패 (시도 {attempt + 1}/{self.retry_count}): {response.status}")
                                if attempt < self.retry_count - 1:
                                    await asyncio.sleep(self.retry_delay)
                except Exception as e:
                    logger.error(f"텔레그램 메시지 전송 중 에러 발생 (시도 {attempt + 1}/{self.retry_count}): {str(e)}")
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay)
            
            return False
            
        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 치명적 에러 발생: {str(e)}")
            return False

    async def get_youtube_trends(self, region_code: str = "KR") -> Optional[List[TrendItem]]:
        """유튜브 트렌드를 가져오는 함수"""
        logger.info(f"유튜브 트렌드 수집 시작... (국가: {region_code})")
        
        try:
            self.init_youtube()
            
            for attempt in range(self.retry_count):
                try:
                    request = self.youtube.videos().list(
                        part="snippet,statistics",
                        chart="mostPopular",
                        regionCode=region_code,
                        maxResults=10
                    )
                    response = request.execute()
                    
                    trends_data = []
                    for idx, item in enumerate(response['items'], 1):
                        title = item['snippet']['title']
                        channel = item['snippet']['channelTitle']
                        views = int(item['statistics']['viewCount'])
                        video_id = item['id']
                        video_url = f"https://youtu.be/{video_id}"
                        
                        # 조회수 포맷팅
                        views_str = self.format_views(views)
                        
                        trend_item = TrendItem(
                            title=title,
                            rank=idx,
                            channel=channel,
                            views=views_str,
                            url=video_url
                        )
                        trends_data.append(trend_item)
                    
                    logger.info(f"유튜브 트렌드 수집 완료 (국가: {region_code})")
                    return trends_data
                    
                except Exception as e:
                    logger.error(f"유튜브 트렌드 수집 중 에러 발생 (시도 {attempt + 1}/{self.retry_count}): {str(e)}")
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay)
            
            return None
                
        except Exception as e:
            logger.error(f"유튜브 트렌드 수집 중 치명적 에러 발생 ({region_code}): {str(e)}")
            return None

    @staticmethod
    def format_views(views: int) -> str:
        """조회수 포맷팅"""
        if views >= 10000000:  # 1천만 이상
            return f"{views/10000000:.1f}천만"
        elif views >= 100000:  # 10만 이상
            return f"{views/10000:.1f}만"
        else:
            return f"{views:,}"  # 천 단위 구분자

    async def collect_and_save_trends(self):
        """트렌드 데이터를 수집하는 함수"""
        logger.info("\n=== 유튜브 트렌드 데이터 수집 시작 ===")
        current_time = get_korea_time().strftime("%Y-%m-%d %H:%M")
        logger.info(f"수집 시작 시간: {current_time}")
        
        collected_data = {
            "KR": None,
            "US": None
        }
        
        try:
            # 1. 유튜브 트렌드 한국
            kr_youtube_trends = await self.get_youtube_trends("KR")
            if kr_youtube_trends is not None:
                collected_data["KR"] = kr_youtube_trends
                logger.info("한국 유튜브 트렌드 데이터 수집 완료")
                await asyncio.sleep(5)

            # 2. 유튜브 트렌드 미국
            us_youtube_trends = await self.get_youtube_trends("US")
            if us_youtube_trends is not None:
                collected_data["US"] = us_youtube_trends
                logger.info("미국 유튜브 트렌드 데이터 수집 완료")
            
            logger.info("=== 데이터 수집 완료 ===\n")
            return collected_data
            
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            logger.error(f"치명적 에러 발생: {error_message}")
            return None

    async def send_trends_updates(self, collected_data):
        """트렌드 데이터를 전송하는 함수"""
        try:
            # 1. 업데이트 시간 체크
            current_time = get_korea_time()
            current_hour = current_time.hour
            
            logger.info(f"현재 시간: {current_time.strftime('%Y-%m-%d %H:%M')}")
            
            # 1시에는 일일 요약 전송
            if is_daily_summary_time():
                logger.info("일일 요약 시간입니다.")
                for country in ["KR", "US"]:
                    summary = self._format_daily_summary(country)
                    await self.send_telegram_message(summary)
                    logger.info(f"{country} 유튜브 트렌드 일일 요약 전송 완료")
                return
            
            # 업데이트 시간이 아니면 종료
            if not is_update_time():
                logger.info("현재는 업데이트 시간이 아닙니다.")
                night_message = self._format_night_mode_message()
                await self.send_telegram_message(night_message)
                return

            # 2. 매일 6시에는 전체 데이터 전송
            if is_daily_update_time():
                logger.info("일일 전체 업데이트 시간입니다.")
                # 6시에는 URL 추적 초기화
                self._reset_sent_urls()
                
                for country in ["KR", "US"]:
                    if collected_data[country]:
                        message = self._format_full_trends_message(country, collected_data[country])
                        await self.send_telegram_message(message)
                        logger.info(f"{country} 유튜브 트렌드 전체 데이터 전송 완료")
                        
                        # 전송한 URL을 sent_urls에 추가
                        for item in collected_data[country]:
                            self.sent_urls.add(item.url)
                        self._save_sent_urls()  # URL 저장
                        
                        self._save_trends_data(country, collected_data[country])
                return

            # 3. 한국 트렌드 처리
            if collected_data["KR"]:
                old_data = self._load_trends_data("KR")
                logger.info(f"이전 데이터 개수: {len(old_data)}, 새로운 데이터 개수: {len(collected_data['KR'])}")
                
                # 이전 데이터가 없는 경우에만 전체 데이터 전송
                if not old_data:
                    logger.info("이전 데이터가 없습니다. 전체 데이터를 전송합니다.")
                    message = self._format_full_trends_message("KR", collected_data["KR"])
                    await self.send_telegram_message(message)
                    
                    # 전송한 URL을 sent_urls에 추가
                    for item in collected_data["KR"]:
                        self.sent_urls.add(item.url)
                    self._save_sent_urls()  # URL 저장
                    
                    self._save_trends_data("KR", collected_data["KR"])
                else:
                    # 변경사항 감지
                    changes = self._detect_changes(old_data, collected_data["KR"])
                    
                    # 변경사항이 있는 경우에만 메시지 전송
                    message = self._format_changes_message(changes, "KR")
                    if message is not None:  # None이 아닌 경우에만 전송
                        await self.send_telegram_message(message)
                        logger.info("한국 유튜브 트렌드 변경사항 전송 완료")
                        
                        # 모든 URL 추가 (중복 방지는 set에서 자동으로 처리)
                        for item in collected_data["KR"]:
                            self.sent_urls.add(item.url)
                        self._save_sent_urls()  # URL 저장
                        
                        self._save_trends_data("KR", collected_data["KR"])
                    else:
                        logger.info("한국 유튜브 트렌드 변경사항 없음")
                
                await asyncio.sleep(5)

            # 4. 미국 트렌드 처리
            if collected_data["US"]:
                old_data = self._load_trends_data("US")
                logger.info(f"이전 데이터 개수: {len(old_data)}, 새로운 데이터 개수: {len(collected_data['US'])}")
                
                # 이전 데이터가 없는 경우에만 전체 데이터 전송
                if not old_data:
                    logger.info("이전 데이터가 없습니다. 전체 데이터를 전송합니다.")
                    message = self._format_full_trends_message("US", collected_data["US"])
                    await self.send_telegram_message(message)
                    
                    # 전송한 URL을 sent_urls에 추가
                    for item in collected_data["US"]:
                        self.sent_urls.add(item.url)
                    self._save_sent_urls()  # URL 저장
                    
                    self._save_trends_data("US", collected_data["US"])
                else:
                    # 변경사항 감지
                    changes = self._detect_changes(old_data, collected_data["US"])
                    
                    # 변경사항이 있는 경우에만 메시지 전송
                    message = self._format_changes_message(changes, "US")
                    if message is not None:  # None이 아닌 경우에만 전송
                        await self.send_telegram_message(message)
                        logger.info("미국 유튜브 트렌드 변경사항 전송 완료")
                        
                        # 모든 URL 추가 (중복 방지는 set에서 자동으로 처리)
                        for item in collected_data["US"]:
                            self.sent_urls.add(item.url)
                        self._save_sent_urls()  # URL 저장
                        
                        self._save_trends_data("US", collected_data["US"])
                    else:
                        logger.info("미국 유튜브 트렌드 변경사항 없음")
        
            # 첫 실행인 경우 상태 업데이트
            if self.is_first_run:
                self.is_first_run = False
                self._save_first_run_state()
        
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            logger.error(f"치명적 에러 발생: {error_message}")

async def scheduled_job(bot: YouTubeTrendsBot):
    """정해진 시간에 실행될 작업"""
    # 데이터 수집 및 저장
    collected_data = await bot.collect_and_save_trends()
    
    # 업데이트 시간인 경우에만 전송
    if is_update_time() and collected_data:
        await bot.send_trends_updates(collected_data)

def run_scheduler():
    """스케줄러 실행"""
    logger.info("=== 유튜브 트렌드 봇 시작 ===")
    logger.info("4시간마다 유튜브 트렌드를 수집하여 텔레그램으로 전송합니다.")
    logger.info("업데이트 시간: 6시 ~ 1시 (한국 시간)")
    logger.info("매일 6시에는 전체 데이터를 전송합니다.")
    
    # 디버그 모드 로그
    if DEBUG_MODE:
        logger.info("현재 디버그 모드입니다. 테스트용 설정이 적용됩니다.")
    
    # 봇 인스턴스 생성
    bot = YouTubeTrendsBot()
    
    # 시작할 때 한 번 실행
    asyncio.run(scheduled_job(bot))
    
    # 4시간마다 실행 (0시, 4시, 8시, 12시, 16시, 20시)
    schedule.every(4).hours.at(":00").do(lambda: asyncio.run(scheduled_job(bot)))
    
    while True:
        schedule.run_pending()
        time.sleep(10)  # 10초마다 체크

if __name__ == "__main__":
    run_scheduler() 