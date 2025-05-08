# Google + YouTube Trends Bot

실시간 구글/YouTube 트렌드를 수집하여 텔레그램으로 전송하는 봇입니다.

## 기능
- 한국과 미국의 실시간 구글/YouTube 트렌드 수집
- 시간별 자동 업데이트
- 텔레그램 메시지 전송

## 설치 방법

1. 저장소 클론
```bash
git clone https://github.com/blueblud7/googletrends.git
cd googletrends
```

2. 필요한 패키지 설치
```bash
pip install -r requirements.txt
```

3. 환경 변수 설정
- `.env.example` 파일을 `.env.local`로 복사
- `.env.local` 파일에 텔레그램 봇 토큰과 채팅 ID 입력
```bash
cp .env.example .env.local
```

## 실행 방법
```bash
python trends_bot.py
```

## 환경 변수 설정
- `TELEGRAM_BOT_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID`: 텔레그램 채팅 ID 
