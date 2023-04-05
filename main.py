import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import sqlite3
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from emoji_rank import rank_emoji
from flask import Flask, render_template, request, jsonify, escape, Markup
from multiprocessing import Process
import time
import threading
import schedule
from dotenv import load_dotenv

load_dotenv()

bot_token = os.environ.get('SLACK_BOT_TOKEN')
app_token = os.environ.get('SLACK_APP_TOKEN')

app = App(token=bot_token)
flask_app = Flask(__name__)
flask_app.template_folder = 'templates'
db_lock = threading.Lock()

def schedule_set():
    # 자정에 set_users() 함수를 호출하도록 예약
    schedule.every().day.at("00:00").do(set_users)

    while True:
        # 예약된 작업을 실행
        schedule.run_pending()
        time.sleep(60)  # 1분마다 예약된 작업을 확인하도록 설정

def get_current_date():
    return time.strftime('%Y-%m-%d', time.localtime(time.time()))

def set_users():
    # 데이터베이스 연결 및 사용자 테이블 생성
    client = WebClient(token=bot_token)
    conn = sqlite3.connect("slack.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, real_name TEXT, display_name TEXT, email TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, daily_emoji_limit INTEGER DEFAULT 10)")
    try:
        # Slack API를 사용하여 사용자 목록을 가져옵니다.
        # 초기화할 net_count 값을 설정합니다.
        init_net_count = 10
        current_time = get_current_date()
        response = client.users_list()
        users = response['members']

         # 기존 테이블을 삭제하고 새로운 스키마로 다시 생성
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, real_name TEXT, display_name TEXT, email TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, daily_emoji_limit INTEGER DEFAULT 10)")

        # 사용자 정보를 데이터베이스에 저장합니다.
        for user in users:
            user_id = user['id']
            name = user['name']

            real_name = user.get('real_name', name)
            
            profile = user['profile']
            display_name = profile.get('display_name', user.get('real_name', name))

            # if not real_name:
            #     real_name = user_id

            if not display_name:
                display_name = real_name

            email = user['profile']['email'] if 'email' in user['profile'] else None

            cursor.execute("INSERT OR REPLACE INTO users (id, name, real_name, display_name, email, updated_at, daily_emoji_limit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (user_id, name, real_name, display_name, email, current_time, init_net_count))

        # 변경 사항을 커밋하고 데이터베이스 연결을 닫습니다.
        conn.commit()
        print("Users saved to the database.")
    except SlackApiError as e:
        print(f"Error: {e}")

    finally:
        conn.close()

def upload_file(channel_id, file_path, message_text):
    try:
        client = WebClient(token=bot_token)
        client.files_upload(
            channels=channel_id,
            file=file_path,
            initial_comment=message_text,
        )
    except SlackApiError as e:
        print(f"Error uploading file: {e}")


def handle_reaction_event(body, logger, event_type):
    event = body["event"]
    
    # 이벤트에서 정보 추출
    user = event["user"]
    reaction = event["reaction"]
    item_user = event.get("item_user", None)
    timestamp = event["event_ts"]

    # 이모지 사용 정보를 데이터베이스에 저장
    with db_lock:
        try:
            conn = sqlite3.connect("slack.db")
            cursor = conn.cursor()
            client = WebClient(token=bot_token)
            # 이모지가 +1 추가 부분
            if reaction == "+1" and event_type == "added":
                if user != item_user:
                    cursor.execute("SELECT daily_emoji_limit FROM users WHERE id = ?", (user,))
                    result = cursor.fetchone()
                    if result:
                        daily_emoji_limit = result[0]
                        # daily_emoji_limit 가 0보다 작거나 같으면
                        if daily_emoji_limit <= 0:
                        # 해당 유저에게만 보이는 메시지 전달
                            try:
                                client.chat_postMessage(
                                    channel=user,
                                    text="하루 사용 가능한 따봉 이모지를 모두 사용하셨습니다. <http://10.10.55.151:3000/|랭킹 페이지>"
                                )
                            except SlackApiError as e:
                                logger.error(f"Error sending ephemeral message: {e}")                
                        else:
                            # 점수를 차감
                            daily_emoji_limit -= 1
                            #DB에 저장 (이모지 기록)
                            cursor.execute("INSERT INTO emoji_usage (user_id, item_user_id, timestamp, reaction, event_type) VALUES (?, ?, ?, ?, ?)",
                            (user, item_user, timestamp, reaction, event_type))
                            #DB에 저장 (오늘 남은 따봉 이모지 갯수)
                            cursor.execute("UPDATE users SET daily_emoji_limit = ? WHERE id = ?", (daily_emoji_limit, user))
                            try:
                                client.chat_postMessage(
                                channel=user,
                                text=f"오늘 사용 가능한 따봉 이모지는 {daily_emoji_limit}개 남았습니다. <http://10.10.55.151:3000/|랭킹 페이지>"
                            )
                            except SlackApiError as e:
                                logger.error(f"Error sending ephemeral message: {e}")
                else:
                    try:
                        client.chat_postMessage(
                            channel=user,
                            text="본인에게 준 따봉은 <http://10.10.55.151:3000/|랭킹>에 반영되지 않습니다."
                        )
                    except SlackApiError as e:
                        logger.error(f"Error sending ephemeral message: {e}")

                        
            # 이모지가 +1 삭제 부분
            elif reaction == "+1" and event_type == "removed":
                try:
                    client.chat_postMessage(
                    channel=user,
                    text="따봉 삭제 기록은 <http://10.10.55.151:3000/|랭킹>에 저장되지 않습니다."
                    )
                except SlackApiError as e:
                    logger.error(f"Error sending ephemeral message: {e}")
            
            conn.commit()
            
        except Exception as e:
            print(f"DB 업데이트 중 오류 발생: {e}")
    
        finally:
            conn.close()
            logger.info(f"{user} {event_type} '{reaction}' emoji to {item_user}'s message at {timestamp}")

@app.event("app_mention")
def handle_mention(event, say):
    text = f"Hello, <@{event['user']}>!"
    say(text=text)

@app.command("/초기화")
def handle_command(ack, command, logger, say):
    """
    초기화 커맨드 핸들러. set_users() 함수를 호출하고 결과 메시지를 출력합니다.
    """
    user_id = command["user_id"]
    authorized_user_id = "UALNQ72F7"

    # 커맨드를 인지했다는 응답을 전송
    ack()
    
    # 커맨드 정보 로깅
    logger.info(f"Command: {command}")

    if user_id == authorized_user_id:
        # 사용자 목록 초기화
        set_users()

        # 초기화 완료 메시지 전송
        message_text = "초기화가 완료되었습니다."
    else:
        # 권한 없음 메시지 전송
        message_text = "사용할 수 있는 권한이 없습니다."

    say(text=message_text, response_type="ephemeral")

@app.command("/랭킹")
def handle_command(say, ack, command, logger):
    ack()
    fin_text = rank_emoji(call_type = "slack")
    say(text=fin_text)

    
    logger.info(f"Command: {command}")

def handle_ranking(ack, command, logger):
    channel_id = command["channel_id"]
    file_path = "ranking.txt"
    message_text = "랭킹입니다."
    ack()
    upload_file(channel_id, file_path, message_text)
    
    logger.info(f"Command: {command}")

@app.event("reaction_added")
def handle_reaction_added(body, logger):
    handle_reaction_event(body, logger, "added")

@app.event("reaction_removed")
def handle_reaction_removed(body, logger):
    handle_reaction_event(body, logger, "removed")

@flask_app.route('/')
def home():
    return render_template('home.html')

@flask_app.route('/rank_emoji', methods=['POST'])
def get_rank_emoji():

    fin_text = rank_emoji(call_type="flask")
    fin_text = Markup(fin_text)
    return render_template('home.html', result=fin_text)

# Flask app 실행을 위한 함수
def run_flask():
    flask_app.run(debug=False, host = "10.10.55.151", port=3000)

def run_slackapp():
    handler = SocketModeHandler(app, app_token)
    handler.start()

if __name__ == "__main__":
    
    slack_process = Process(target=run_slackapp)
    slack_process.start()
    
    # Flask app 실행
    flask_process = Process(target=run_flask)
    flask_process.start()

    # 스케줄링 프로세스 실행
    schedule_process = Process(target=schedule_set)
    schedule_process.start()

    # 프로세스 종료를 위해 대기
    flask_process.join()
    slack_process.join()
    schedule_process.join()

