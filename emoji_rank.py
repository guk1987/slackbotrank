import sqlite3
from datetime import datetime, timedelta

def get_date_range():

    # 현재 날짜 구하기
    today = datetime.today()

    # 이번 달의 첫 번째 날 구하기
    first_day = today.replace(day=1)

    # 다음 달의 첫 번째 날 구하기
    next_month = (today.month % 12) + 1
    next_year = today.year + (today.month + 1 > 12)
    first_day_of_next_month = today.replace(day=1, month=next_month, year=next_year)

    # 이번 달의 마지막 날 구하기
    last_day = first_day_of_next_month - timedelta(days=1)

    # 문자열로 변환하여 출력하기
    first_day_str = first_day.strftime('%Y-%m-%d')
    last_day_str = last_day.strftime('%Y-%m-%d')

    print('이번 달의 첫 번째 날:', first_day_str)
    print('이번 달의 마지막 날:', last_day_str)
    return first_day_str, last_day_str

def rank_emoji(call_type):
    start_date, end_date = get_date_range()
    fin_text=""
    fin_html=""
    conn = sqlite3.connect("slack.db")
    cursor = conn.cursor()

    # 이모지를 많이 받은 사용자별 이모지 누적 개수
    query = """
        SELECT 
            eu.item_user_id, 
            u.display_name,
            strftime('%Y-%m-%d', eu.timestamp, 'unixepoch') as date,
            SUM(CASE WHEN event_type = 'added' AND reaction = '+1' THEN 1 ELSE 0 END) as sum_added_count,
            SUM(CASE WHEN event_type = 'removed' AND reaction = '+1' THEN 1 ELSE 0 END) as sum_removed_count,
            MIN(CASE WHEN event_type = 'added' AND reaction = '+1' THEN eu.timestamp ELSE NULL END) as first_added_ts,
            MAX(CASE WHEN event_type = 'added' AND reaction = '+1' THEN eu.timestamp ELSE NULL END) as last_added_ts,
            SUM(CASE WHEN event_type = 'added' AND reaction = '+1' THEN 1 ELSE 0 END) - SUM(CASE WHEN event_type = 'removed' AND reaction = '+1' THEN 1 ELSE 0 END) as net_count
        FROM emoji_usage eu
        LEFT JOIN users u ON eu.item_user_id = u.id
        WHERE date(eu.timestamp, 'unixepoch') BETWEEN ? AND ?
        GROUP BY eu.item_user_id
        HAVING net_count != 0
        ORDER BY net_count DESC, date DESC;
    """
    
    cursor.execute(query, (start_date, end_date))

    emoji_received = cursor.fetchall()

    fin_text+="*따봉을 많이 받은 사용자 순위:*\n"
    fin_html+='''
    <div class="container">
    <div class="table-container">
    <h3>따봉을 많이 받은 사용자 순위</h3>
            <table>
        <thead>
            <tr>
                <th>순위</th>
                <th>이름</th>
                <th>받은 개수</th>
            </tr>
        </thead>
        <tbody>'''
    rank = 1
    for row in emoji_received:
        user_id, display_name, date, sum_added_count, sum_removed_count, first_added_ts, last_added_ts, net_count = row
        if display_name == '':
            display_name = f"{user_id}(ID)"
        fin_text+=f">{rank}위 - {display_name}({net_count }개)\n"
        fin_html+=f'''
            <tr>
                <td>{rank}</td>
                <td>{display_name}</td>
                <td>{net_count}</td>
            </tr>
        '''
        rank += 1
    fin_html+='''</tbody></table></div>'''
    # 이모지를 많이 준 사용자별 이모지 누적 개수
    query = """
        SELECT 
            eu.user_id, 
            u.display_name,
            strftime('%Y-%m-%d', eu.timestamp, 'unixepoch') as date,
            SUM(CASE WHEN event_type = 'added' AND reaction = '+1' THEN 1 ELSE 0 END) as sum_added_count,
            SUM(CASE WHEN event_type = 'removed' AND reaction = '+1' THEN 1 ELSE 0 END) as sum_removed_count,
            MIN(CASE WHEN event_type = 'added' AND reaction = '+1' THEN eu.timestamp ELSE NULL END) as first_added_ts,
            MAX(CASE WHEN event_type = 'added' AND reaction = '+1' THEN eu.timestamp ELSE NULL END) as last_added_ts,
            SUM(CASE WHEN event_type = 'added' AND reaction = '+1' THEN 1 ELSE 0 END) - SUM(CASE WHEN event_type = 'removed' AND reaction = '+1' THEN 1 ELSE 0 END) as net_count
        FROM emoji_usage eu
        LEFT JOIN users u ON eu.user_id = u.id
        WHERE date(eu.timestamp, 'unixepoch') BETWEEN ? AND ?
        GROUP BY eu.user_id
        HAVING net_count != 0
        ORDER BY net_count DESC, date DESC;
    """
    cursor.execute(query, (start_date, end_date))
    emoji_given = cursor.fetchall()

    fin_text+="\n*따봉을 많이 준 사용자 순위:*\n"
    fin_html+='''
    <div class="table-container">
    <h3>따봉을 많이 준 사용자 순위</h3>
        <table>
        <thead>
            <tr>
                <th>순위</th>
                <th>이름</th>
                <th>사용 개수</th>
            </tr>
        </thead>
        <tbody>
    '''
    #순위 변수에 1을 넣어준다.
    rank = 1
    for row in emoji_given:
        user_id, display_name, date, sum_added_count, sum_removed_count, first_added_ts, last_added_ts, net_count = row
            
        fin_text+=f">{rank}위 - {display_name}({net_count }개)\n"
        fin_html+=f'''
            <tr>
                <td>{rank}</td>
                <td>{display_name}</td>
                <td>{net_count}</td>
            </tr>
          '''
        rank += 1
    
    
    fin_html+='''</tbody></table></div></div>'''

    conn.close()

    if call_type == "slack":
        return fin_text

    elif call_type =="flask":
        return fin_html, start_date, end_date

if __name__ == "__main__":

    test_text = rank_emoji(call_type="slack")
    print(test_text)
