import os
import asyncio
import schedule
import time
import json
import logging
import aiohttp
import requests
import xml.etree.ElementTree as ET
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
        logging.FileHandler('unified_trends.log'),
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
        logger.info(message)

# API 키 설정
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
YOUTUBE_CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID')  # 유튜브 전용 채널 ID
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# 환경 변수 디버그 출력
debug_print("환경 변수 로드 상태:")
debug_print(f"TELEGRAM_BOT_TOKEN: {'설정됨' if TELEGRAM_BOT_TOKEN else '설정되지 않음'}")
debug_print(f"TELEGRAM_CHAT_ID: {'설정됨' if TELEGRAM_CHAT_ID else '설정되지 않음'}")
debug_print(f"YOUTUBE_CHANNEL_ID: {'설정됨' if YOUTUBE_CHANNEL_ID else '설정되지 않음'}")
debug_print(f"YOUTUBE_API_KEY: {'설정됨' if YOUTUBE_API_KEY else '설정되지 않음'}")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID]):
    missing_vars = []
    if not TELEGRAM_BOT_TOKEN: missing_vars.append('TELEGRAM_BOT_TOKEN')
    if not TELEGRAM_CHAT_ID: missing_vars.append('TELEGRAM_CHAT_ID')
    if not YOUTUBE_API_KEY: missing_vars.append('YOUTUBE_API_KEY')
    if not YOUTUBE_CHANNEL_ID: missing_vars.append('YOUTUBE_CHANNEL_ID')
    raise ValueError(f"다음 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}. .env.local 파일을 확인해주세요.")

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

@dataclass
class TrendItem:
    """트렌드 아이템 데이터 클래스"""
    title: str
    rank: int
    source: str = ""  # 출처 (구글/유튜브)
    description: str = ""  # 추가 설명 (트래픽, 조회수 등)
    url: str = ""  # 링크

class UnifiedTrendsBot:
    def __init__(self):
        self.youtube = None
        self.retry_count = 3
        self.retry_delay = 5
        self.data_dir = Path("unified_trends_data")
        self.data_dir.mkdir(exist_ok=True)
        self.is_first_run = self._load_first_run_state()
        self.sent_items = set()  # 전송된 항목을 추적하는 세트
        self._load_sent_items()  # 초기화시 항목 로드

    def _get_data_file_path(self, source: str, country: str) -> Path:
        """데이터 파일 경로 반환"""
        return self.data_dir / f"{source}_{country}.json"

    def _get_first_run_file_path(self) -> Path:
        """첫 실행 상태 파일 경로 반환"""
        return self.data_dir / "first_run.json"

    def _get_sent_items_file_path(self) -> Path:
        """전송된 항목 파일 경로 반환"""
        return self.data_dir / "sent_items.json"

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

    def _load_sent_items(self):
        """전송된 항목 로드"""
        file_path = self._get_sent_items_file_path()
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    self.sent_items = set(json.load(f))
                logger.info(f"전송된 항목 {len(self.sent_items)}개를 로드했습니다.")
            except Exception as e:
                logger.error(f"전송된 항목 로드 실패: {str(e)}")
                self.sent_items = set()
        else:
            logger.info("전송된 항목 파일이 없습니다. 새로운 세트를 생성합니다.")
            self.sent_items = set()

    def _save_sent_items(self):
        """전송된 항목 저장"""
        file_path = self._get_sent_items_file_path()
        try:
            with open(file_path, 'w') as f:
                json.dump(list(self.sent_items), f)
            logger.info(f"전송된 항목 {len(self.sent_items)}개를 저장했습니다.")
        except Exception as e:
            logger.error(f"전송된 항목 저장 실패: {str(e)}")

    def _reset_sent_items(self):
        """전송된 항목 초기화 (매일 6시에 호출)"""
        self.sent_items.clear()
        self._save_sent_items()
        logger.info("전송된 항목을 초기화했습니다.")

    def _save_trends_data(self, source: str, country: str, data: List[TrendItem]):
        """트렌드 데이터 저장"""
        file_path = self._get_data_file_path(source, country)
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([item.__dict__ for item in data], f, ensure_ascii=False, indent=2)
            logger.info(f"{source}_{country} 트렌드 데이터 저장 완료")
        except Exception as e:
            logger.error(f"{source}_{country} 트렌드 데이터 저장 실패: {str(e)}")

    def _load_trends_data(self, source: str, country: str) -> List[TrendItem]:
        """트렌드 데이터 로드"""
        file_path = self._get_data_file_path(source, country)
        try:
            if not file_path.exists():
                logger.info(f"{source}_{country} 트렌드 데이터 파일이 없습니다.")
                return []
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"{source}_{country} 트렌드 데이터 로드 완료")
                return [TrendItem(**item) for item in data]
        except Exception as e:
            logger.error(f"{source}_{country} 트렌드 데이터 로드 실패: {str(e)}")
            return []

    def _detect_changes(self, old_data: List[TrendItem], new_data: List[TrendItem]) -> Dict[str, List[Any]]:
        """트렌드 변경 감지"""
        changes = {
            'new': [],
            'up': [],    # 순위 상승
            'down': [],  # 순위 하락
            'same': []   # 순위 동일
        }

        # 이전 데이터를 딕셔너리로 변환 (key: title, value: 순위)
        old_items = {item.title: item.rank for item in old_data}
        
        logger.info(f"이전 데이터: {len(old_items)}개")
        
        # 새로운 항목과 순위 변경 감지
        for new_item in new_data:
            if new_item.title in old_items:
                old_rank = old_items[new_item.title]
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

    def _format_changes_message(self, changes: Dict[str, List[Any]], source: str, country: str) -> Optional[str]:
        """변경사항 메시지 포맷팅"""
        # 변경사항이 없으면 None 반환
        if not (changes['new'] or changes['up'] or changes['down']):
            logger.info("변경사항이 없어 메시지를 전송하지 않습니다.")
            return None

        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        source_emoji = "🔍" if source == "google" else "📺"
        source_name = "구글 트렌드" if source == "google" else "유튜브 트렌드"
        
        current_time = get_korea_time()
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"{source_emoji} {country_emoji} {country_name} {source_name} 업데이트 ({date_str})\n"
        message += "📊 순위 변경 및 신규 진입\n\n"
        
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
        
        # 변경사항이 없으면 None 반환
        if not all_changes:
            logger.info("필터링 후 변경사항이 없어 메시지를 전송하지 않습니다.")
            return None
            
        # 순위로 정렬
        all_changes.sort(key=lambda x: x['item'].rank)
        
        # 변경사항 포맷팅
        for change in all_changes:
            item = change['item']
            
            if change['type'] == 'new':
                message += f"{item.rank}위) {item.title} New\n"
            elif change['type'] == 'up':
                message += f"{item.rank}위) {item.title} {change['old_rank']} → {change['new_rank']}\n"
            elif change['type'] == 'down':
                message += f"{item.rank}위) {item.title} {change['old_rank']} → {change['new_rank']}\n"
            
            message += f"{item.description}\n"
            if item.url:
                message += f"🔗 {item.url}\n"
            message += "\n"
        
        return message

    def _format_full_trends_message(self, source: str, country: str, data: List[TrendItem]) -> str:
        """전체 트렌드 데이터 메시지 포맷팅"""
        country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
        country_name = "한국" if country == "KR" else "미국"
        source_emoji = "🔍" if source == "google" else "📺"
        source_name = "구글 트렌드" if source == "google" else "유튜브 트렌드"
        
        current_time = get_korea_time()
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        weekday = weekdays[current_time.weekday()]
        date_str = current_time.strftime(f"%Y-%m-%d {weekday}요일")
        
        message = f"{source_emoji} {country_emoji} {country_name} {source_name} ({date_str})\n\n"
        
        for item in data:
            message += f"{item.rank}위) {item.title}\n"
            message += f"{item.description}\n"
            if item.url:
                message += f"🔗 {item.url}\n"
            message += "\n"
        
        return message

    def init_youtube(self):
        """YouTube API 클라이언트 초기화"""
        if not self.youtube:
            self.youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

    async def send_telegram_message(self, message: str, is_youtube: bool = False) -> bool:
        """텔레그램으로 메시지를 전송하는 함수"""
        try:
            # 기본 채널로 전송
            success = await self._send_to_channel(TELEGRAM_CHAT_ID, message)
            
            # 유튜브 트렌드인 경우 추가 채널로도 전송
            if is_youtube:
                youtube_success = await self._send_to_channel(YOUTUBE_CHANNEL_ID, message)
                return success and youtube_success
            
            return success
            
        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 치명적 에러 발생: {str(e)}")
            return False

    async def _send_to_channel(self, chat_id: str, message: str) -> bool:
        """특정 채널로 메시지를 전송하는 내부 함수"""
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            for attempt in range(self.retry_count):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, json=data) as response:
                            if response.status == 200:
                                logger.info(f"텔레그램 메시지 전송 성공 (채널: {chat_id})")
                                return True
                            else:
                                logger.error(f"텔레그램 메시지 전송 실패 (채널: {chat_id}, 시도 {attempt + 1}/{self.retry_count}): {response.status}")
                                if attempt < self.retry_count - 1:
                                    await asyncio.sleep(self.retry_delay)
                except Exception as e:
                    logger.error(f"텔레그램 메시지 전송 중 에러 발생 (채널: {chat_id}, 시도 {attempt + 1}/{self.retry_count}): {str(e)}")
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay)
            
            return False
            
        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 치명적 에러 발생 (채널: {chat_id}): {str(e)}")
            return False

    async def get_google_trends(self, country: str) -> List[TrendItem]:
        """구글 트렌드 RSS 피드를 가져오는 함수"""
        logger.info(f"구글 트렌드 수집 시작 (국가: {country})...")
        
        try:
            # RSS 피드 URL
            rss_url = f"https://trends.google.com/trending/rss?geo={country}"
            response = requests.get(rss_url)
            
            if response.status_code == 200:
                # XML 파싱
                root = ET.fromstring(response.content)
                channel = root.find('channel')
                items = channel.findall('item')
                
                trends_data = []
                for idx, item in enumerate(items, 1):
                    title = item.find('title').text
                    traffic = item.find('{https://trends.google.com/trending/rss}approx_traffic').text
                    
                    # 관련 뉴스 수집
                    news_items = item.findall('{https://trends.google.com/trending/rss}news_item')
                    news_info = ""
                    news_url = ""
                    
                    if news_items:
                        first_news = news_items[0]
                        news_title = first_news.find('{https://trends.google.com/trending/rss}news_item_title').text
                        news_url = first_news.find('{https://trends.google.com/trending/rss}news_item_url').text
                        news_source = first_news.find('{https://trends.google.com/trending/rss}news_item_source').text
                        news_info = f"📰 {news_title} | 📱 {news_source}"
                    
                    trend_item = TrendItem(
                        title=title,
                        rank=idx,
                        source="google",
                        description=f"🔍 {traffic} | {news_info}",
                        url=news_url
                    )
                    trends_data.append(trend_item)
                
                logger.info(f"{country} 구글 트렌드 수집 완료")
                return trends_data
                
            else:
                logger.error(f"RSS 피드 요청 실패: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"구글 트렌드 수집 중 에러 발생 ({country}): {str(e)}")
            return []

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
                            source="youtube",
                            description=f"👤 {channel} | 👁 {views_str}회",
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

    async def process_trends_data(self, source: str, country: str, data: List[TrendItem]):
        """트렌드 데이터 처리 및 전송"""
        try:
            # 1. 이전 데이터 로드
            old_data = self._load_trends_data(source, country)
            logger.info(f"이전 데이터 개수: {len(old_data)}, 새로운 데이터 개수: {len(data)}")
            
            # 2. 매일 6시 또는 이전 데이터가 없는 경우 전체 데이터 전송
            if is_daily_update_time() or not old_data:
                if is_daily_update_time():
                    logger.info("일일 전체 업데이트 시간입니다.")
                else:
                    logger.info("이전 데이터가 없습니다. 전체 데이터를 전송합니다.")
                
                message = self._format_full_trends_message(source, country, data)
                success = await self.send_telegram_message(message, source == "youtube")
                
                if success:
                    logger.info(f"{source}_{country} 트렌드 전체 데이터 전송 완료")
                    # 전송한 항목을 sent_items에 추가
                    for item in data:
                        self.sent_items.add(item.title)
                    self._save_sent_items()
                    self._save_trends_data(source, country, data)
                
            else:
                # 3. 변경사항 감지 및 전송
                changes = self._detect_changes(old_data, data)
                
                # 이미 전송된 항목 필터링
                filtered_new = [item for item in changes['new'] if item.title not in self.sent_items]
                changes['new'] = filtered_new
                
                logger.info(f"필터링 후 신규 항목: {len(filtered_new)}개 (원래: {len(changes['new'])}개)")
                
                message = self._format_changes_message(changes, source, country)
                
                if message:
                    success = await self.send_telegram_message(message, source == "youtube")
                    if success:
                        logger.info(f"{source}_{country} 트렌드 변경사항 전송 완료")
                        # 전송한 항목을 sent_items에 추가
                        for item in filtered_new:
                            self.sent_items.add(item.title)
                        self._save_sent_items()
                        self._save_trends_data(source, country, data)
                else:
                    logger.info(f"{source}_{country} 트렌드 변경사항 없음")
            
        except Exception as e:
            logger.error(f"{source}_{country} 트렌드 처리 중 에러 발생: {str(e)}")

    async def collect_and_save_trends(self):
        """트렌드 데이터를 수집하는 함수"""
        logger.info("\n=== 트렌드 데이터 수집 시작 ===")
        current_time = get_korea_time().strftime("%Y-%m-%d %H:%M")
        logger.info(f"수집 시작 시간: {current_time}")
        
        collected_data = {
            "google": {"KR": None, "US": None},
            "youtube": {"KR": None, "US": None}
        }
        
        try:
            # 1. 구글 트렌드 한국
            kr_google_trends = await self.get_google_trends("KR")
            if kr_google_trends:
                collected_data["google"]["KR"] = kr_google_trends
                logger.info("한국 구글 트렌드 데이터 수집 완료")
                await asyncio.sleep(5)

            # 2. 유튜브 트렌드 한국
            kr_youtube_trends = await self.get_youtube_trends("KR")
            if kr_youtube_trends:
                collected_data["youtube"]["KR"] = kr_youtube_trends
                logger.info("한국 유튜브 트렌드 데이터 수집 완료")
                await asyncio.sleep(5)

            # 3. 구글 트렌드 미국
            us_google_trends = await self.get_google_trends("US")
            if us_google_trends:
                collected_data["google"]["US"] = us_google_trends
                logger.info("미국 구글 트렌드 데이터 수집 완료")
                await asyncio.sleep(5)

            # 4. 유튜브 트렌드 미국
            us_youtube_trends = await self.get_youtube_trends("US")
            if us_youtube_trends:
                collected_data["youtube"]["US"] = us_youtube_trends
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
            if not is_update_time():
                logger.info("현재는 업데이트 시간이 아닙니다.")
                return

            # 2. 매일 6시에는 전체 데이터 전송
            if is_daily_update_time():
                logger.info("일일 전체 업데이트 시간입니다.")
                # 6시에는 전송 항목 초기화
                self._reset_sent_items()
                
                for source in ["google", "youtube"]:
                    for country in ["KR", "US"]:
                        if collected_data[source][country]:
                            message = self._format_full_trends_message(source, country, collected_data[source][country])
                            await self.send_telegram_message(message, source == "youtube")
                            logger.info(f"{source}_{country} 트렌드 전체 데이터 전송 완료")
                            
                            # 전송한 항목을 sent_items에 추가
                            for item in collected_data[source][country]:
                                self.sent_items.add(item.title)
                            self._save_sent_items()
                            
                            self._save_trends_data(source, country, collected_data[source][country])
                return

            # 3. 각 소스와 국가별로 변경사항 처리
            for source in ["google", "youtube"]:
                for country in ["KR", "US"]:
                    if collected_data[source][country]:
                        await self.process_trends_data(source, country, collected_data[source][country])
                        await asyncio.sleep(5)  # 5초 대기
        
            # 첫 실행인 경우 상태 업데이트
            if self.is_first_run:
                self.is_first_run = False
                self._save_first_run_state()
        
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            logger.error(f"치명적 에러 발생: {error_message}")

def get_next_scheduled_time():
    """다음 예정된 실행 시간을 계산"""
    current_time = get_korea_time()
    scheduled_hours = [6, 10, 14, 18, 22, 2]  # 4시간 간격의 실행 시간
    
    # 현재 시간보다 큰 다음 실행 시간 찾기
    for hour in scheduled_hours:
        if current_time.hour < hour:
            next_time = current_time.replace(hour=hour, minute=0, second=0, microsecond=0)
            return next_time
    
    # 다음 날 6시로 설정
    next_time = current_time.replace(hour=6, minute=0, second=0, microsecond=0)
    next_time = next_time.replace(day=next_time.day + 1)
    return next_time

async def scheduled_job(bot: UnifiedTrendsBot):
    """정해진 시간에 실행될 작업"""
    # 데이터 수집 및 저장
    collected_data = await bot.collect_and_save_trends()
    
    # 업데이트 시간인 경우에만 전송
    if is_update_time() and collected_data:
        await bot.send_trends_updates(collected_data)

def run_scheduler():
    """스케줄러 실행"""
    logger.info("=== 통합 트렌드 봇 시작 ===")
    logger.info("4시간 간격으로 구글 트렌드와 유튜브 트렌드를 수집하여 텔레그램으로 전송합니다.")
    logger.info("실행 시간: 6시, 10시, 14시, 18시, 22시, 2시 (한국 시간)")
    logger.info("매일 6시에는 전체 데이터를 전송합니다.")
    
    # 디버그 모드 로그
    if DEBUG_MODE:
        logger.info("현재 디버그 모드입니다. 테스트용 설정이 적용됩니다.")
    
    # 봇 인스턴스 생성
    bot = UnifiedTrendsBot()
    
    # 다음 예정된 시간 계산
    next_time = get_next_scheduled_time()
    logger.info(f"다음 실행 예정 시간: {next_time.strftime('%Y-%m-%d %H:%M')}")
    
    # 시작할 때 한 번 실행 (테스트용)
    logger.info("테스트 실행을 시작합니다...")
    asyncio.run(scheduled_job(bot))
    logger.info("테스트 실행이 완료되었습니다.")
    
    # 4시간 간격으로 실행
    schedule.every().day.at("06:00").do(lambda: asyncio.run(scheduled_job(bot)))
    schedule.every().day.at("10:00").do(lambda: asyncio.run(scheduled_job(bot)))
    schedule.every().day.at("14:00").do(lambda: asyncio.run(scheduled_job(bot)))
    schedule.every().day.at("18:00").do(lambda: asyncio.run(scheduled_job(bot)))
    schedule.every().day.at("22:00").do(lambda: asyncio.run(scheduled_job(bot)))
    schedule.every().day.at("02:00").do(lambda: asyncio.run(scheduled_job(bot)))
    
    while True:
        schedule.run_pending()
        time.sleep(10)  # 10초마다 체크

if __name__ == "__main__":
    run_scheduler() 