import os
import asyncio
import schedule
import time
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# 환경 변수 로드 (.env.local)
load_dotenv('.env.local')

# 디버그 모드 설정
DEBUG_MODE = True

def debug_print(message):
    """디버그 메시지 출력"""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

# 텔레그램 봇 설정
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("텔레그램 봇 토큰과 채팅 ID가 설정되지 않았습니다. .env.local 파일을 확인해주세요.")

def send_telegram_message(message):
    """텔레그램으로 메시지를 전송하는 함수"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        print(f"전송 시도 URL: {url}")
        print(f"전송 데이터: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        response = requests.post(url, json=data)
        
        print(f"텔레그램 응답 상태 코드: {response.status_code}")
        print(f"텔레그램 응답 헤더: {dict(response.headers)}")
        print(f"텔레그램 응답 내용: {response.text}")
        
        if response.status_code == 200:
            debug_print("텔레그램 메시지 전송 성공")
        else:
            debug_print(f"텔레그램 메시지 전송 실패: {response.status_code}")
            
    except Exception as e:
        debug_print(f"텔레그램 메시지 전송 중 에러 발생: {str(e)}")
        print(f"상세 에러: {e.__class__.__name__}, {str(e)}")

async def get_google_trends(country):
    """구글 트렌드 RSS 피드를 가져오는 함수"""
    debug_print(f"구글 트렌드 수집 시작 (국가: {country})...")
    
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
                news_data = []
                
                if news_items:
                    first_news = news_items[0]
                    news_title = first_news.find('{https://trends.google.com/trending/rss}news_item_title').text
                    news_url = first_news.find('{https://trends.google.com/trending/rss}news_item_url').text
                    news_source = first_news.find('{https://trends.google.com/trending/rss}news_item_source').text
                    news_data.append(f"📰 {news_title}\n🔗 {news_url}\n📱 {news_source}")
                
                trend_info = f"{idx}위) 🔍 {title} ({traffic})\n" + "\n".join(news_data)
                trends_data.append(trend_info)
            
            country_emoji = "🇰🇷" if country == "KR" else "🇺🇸"
            country_name = "한국" if country == "KR" else "미국"
            formatted_trends = f"{country_emoji} {country_name} 구글 트렌드 (실시간 TOP 10)\n\n" + "\n\n".join(trends_data)
            
            debug_print(f"{country} 트렌드 수집 완료")
            return formatted_trends
            
        else:
            debug_print(f"RSS 피드 요청 실패: {response.status_code}")
            return None
            
    except Exception as e:
        debug_print(f"구글 트렌드 수집 중 에러 발생 ({country}): {str(e)}")
        return None

async def get_trends():
    """트렌드 데이터를 수집하는 메인 함수"""
    debug_print("\n=== 트렌드 데이터 수집 시작 ===")
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    debug_print(f"수집 시작 시간: {current_time}")
    
    try:
        trends_data = []

        # 한국 구글 트렌드 수집
        kr_trends = await get_google_trends('KR')
        if kr_trends is not None:
            trends_data.append(kr_trends)
            debug_print("한국 구글 트렌드 수집 성공")

        # 미국 구글 트렌드 수집
        us_trends = await get_google_trends('US')
        if us_trends is not None:
            trends_data.append(us_trends)
            debug_print("미국 구글 트렌드 수집 성공")
        
        # 전체 데이터 출력 및 텔레그램 전송
        if trends_data:
            message = f"📈 실시간 트렌드 업데이트 ({current_time})\n\n" + "\n\n".join(trends_data)
            debug_print("\n=== 수집된 전체 데이터 ===")
            debug_print(message)
            debug_print("=== 데이터 수집 완료 ===\n")
            
            # 텔레그램으로 전송
            send_telegram_message(message)
        else:
            debug_print("수집된 데이터가 없습니다.")
        
    except Exception as e:
        error_message = f"에러 발생: {str(e)}"
        debug_print(f"치명적 에러 발생: {error_message}")

async def scheduled_job():
    """정해진 시간에 실행될 작업"""
    await get_trends()

def run_scheduler():
    """스케줄러 실행"""
    print("=== 트렌드 봇 시작 ===")
    print("매 시간마다 트렌드를 수집하여 텔레그램으로 전송합니다.")
    
    # 시작할 때 한 번 실행
    asyncio.run(get_trends())
    
    # 매 시간마다 실행
    schedule.every(1).hours.do(lambda: asyncio.run(scheduled_job()))
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    run_scheduler() 