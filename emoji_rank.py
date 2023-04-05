import sqlite3

def rank_emoji(call_type):
    fin_text=""
    fin_html=""
    conn = sqlite3.connect("slack.db")
    cursor = conn.cursor()

    start_date = "2023-04-05"
    end_date = "2023-04-31"

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
        ORDER BY date DESC, net_count DESC;
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
                <th>이름</th>
                <th>받은 개수</th>
            </tr>
        </thead>
        <tbody>'''
    for row in emoji_received:
        user_id, display_name, date, sum_added_count, sum_removed_count, first_added_ts, last_added_ts, net_count = row
        if display_name == '':
            display_name = f"{user_id}(ID)"
        fin_text+=f">{display_name} - {net_count }개\n"
        fin_html+=f'''
            <tr>
                <td>{display_name}</td>
                <td>{net_count}</td>
            </tr>
        '''
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
        ORDER BY date DESC, net_count DESC;
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
                <th>이름</th>
                <th>사용 개수</th>
            </tr>
        </thead>
        <tbody>
    '''
    for row in emoji_given:
        user_id, display_name, date, sum_added_count, sum_removed_count, first_added_ts, last_added_ts, net_count = row
            
        fin_text+=f">{display_name} - {net_count }개\n"
        fin_html+=f'''
            <tr>
              <td>{display_name}</td>
              <td>{net_count}</td>
            </tr>
          '''
    
    
    fin_html+='''</tbody></table></div></div>'''

    conn.close()

    if call_type == "slack":
        return fin_text

    elif call_type =="flask":
        return fin_html

if __name__ == "__main__":

    test_text = rank_emoji(call_type="slack")
    print(test_text)
