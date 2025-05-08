#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from pytrends.request import TrendReq
import pandas as pd
import time
import random

# 디버그 모드 설정
DEBUG_MODE = True
# 업데이트 주기 (초)
UPDATE_INTERVAL = 600  # 10분
# 최대 재시도 횟수
MAX_RETRIES = 3
# 재시도 대기 시간 (초)
RETRY_DELAY = 60

def debug_print(message):
    """디버그 메시지 출력"""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def fetch_trends(region='KR'):
    """특정 지역의 실시간 트렌드를 가져오는 함수"""
    for attempt in range(MAX_RETRIES):
        try:
            debug_print(f"{region} 트렌드 수집 시작... (시도 {attempt + 1}/{MAX_RETRIES})")
            
            # pytrends 초기화 (언어 설정)
            pytrends = TrendReq(hl='ko' if region == 'KR' else 'en-US',
                              timeout=(10,25),  # 연결 타임아웃 설정
                              retries=2,
                              backoff_factor=0.5)
            
            # 현재 시간 정보
            now = datetime.datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')
            
            # 시드 키워드로 시작
            seed_kw = '뉴스' if region == 'KR' else 'news'
            
            # build_payload 호출
            pytrends.build_payload(
                kw_list=[seed_kw],
                timeframe='now 1-H',
                geo=region
            )
            
            # 실시간 급상승 검색어 수집
            suggestions = pytrends.suggestions(seed_kw)
            if suggestions:
                debug_print(f"{region} 트렌드 수집 완료")
                print(f"\n=== {region} 실시간 트렌드 ({date_str} {time_str}) ===")
                
                # 상위 10개 트렌드 출력
                for idx, item in enumerate(suggestions[:10], 1):
                    print(f"{idx}. {item['title']}")
                
                return [item['title'] for item in suggestions[:10]]
            else:
                debug_print(f"{region} 트렌드 데이터가 비어있습니다.")
                
            # 성공적으로 실행되면 루프 종료
            break
                
        except Exception as e:
            debug_print(f"{region} 트렌드 수집 중 에러 발생: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY + random.randint(1, 30)
                debug_print(f"{wait_time}초 후 재시도합니다...")
                time.sleep(wait_time)
            else:
                debug_print(f"{region} 트렌드 수집 실패")
                return None
    
    return None

def clear_screen():
    """화면 지우기"""
    print('\033[2J\033[H', end='')

if __name__ == '__main__':
    print("=== 구글 트렌드 봇 시작 ===")
    print(f"업데이트 주기: {UPDATE_INTERVAL}초")
    
    try:
        while True:
            clear_screen()
            print("\n=== 구글 트렌드 실시간 모니터링 ===")
            
            # 한국 트렌드 수집
            fetch_trends('KR')
            
            time.sleep(5)  # API 호출 간 딜레이 증가
            
            # 미국 트렌드 수집
            fetch_trends('US')
            
            print(f"\n다음 업데이트까지 {UPDATE_INTERVAL}초 대기 중...")
            time.sleep(UPDATE_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다.")