import streamlit as st
import duckdb # 1. pymysql 대신 duckdb 사용
import pandas as pd
import time
import os

# --- 2. DB 연결 설정 ---
# Streamlit 앱이 시작될 때 madang.db 파일에 연결
# 이 파일은 Streamlit Cloud 배포 시 GitHub 리포지토리에 함께 업로드되어야 합니다.
DB_FILE = 'madang.db'
conn = duckdb.connect(database=DB_FILE)

# --- 3. 쿼리 실행 함수 수정 ---
# DuckDB에서는 커서 없이 conn.execute()를 사용하며, 결과를 Pandas DataFrame으로 쉽게 가져올 수 있음
def query(sql, fetch_type='df'):
    """DuckDB에 SQL 쿼리를 실행하고 결과를 반환합니다. 
    fetch_type이 'df'면 DataFrame, 'list'면 리스트를 반환합니다.
    """
    try:
        # SELECT 쿼리 실행 시
        if sql.strip().upper().startswith('SELECT'):
            if fetch_type == 'df':
                return conn.execute(sql).fetchdf() # Pandas DataFrame으로 가져옴
            else:
                return conn.execute(sql).fetchall() # 리스트 형태로 가져옴
        # INSERT, UPDATE 등 데이터 변경 쿼리 실행 시
        else:
            conn.execute(sql)
            # DuckDB는 기본적으로 AUTOCOMMIT이지만, 확실한 반영을 위해 명시적으로 commit
            conn.commit() 
            return None
    except Exception as e:
        st.error(f"데이터베이스 쿼리 실행 중 오류가 발생했습니다: {e}")
        # 오류 발생 시 롤백 (선택 사항)
        # conn.rollback()
        return None

# --- 초기 데이터 로딩 (Book 목록) ---
# 기존의 쿼리 결과 처리 방식에 맞춰서 수정
books = [None]
# CONCAT 함수는 DuckDB에서 ||로 대체 가능하거나 MySQL의 CONCAT을 DuckDB가 이해하지 못할 수 있어, 직접 처리
# SQL: 'SELECT bookid, bookname FROM Book'
result_df = query("SELECT bookid, bookname FROM Book")

if result_df is not None:
    # Pandas를 사용하여 bookid와 bookname을 합친 문자열 리스트 생성
    for index, row in result_df.iterrows():
        books.append(f"{row['bookid']},{row['bookname']}")
else:
    st.error("도서 목록을 불러오는 데 실패했습니다. DB 파일 확인이 필요합니다.")


# --- Streamlit UI 시작 ---
st.title("📚 마당 도서 관리 시스템 (DuckDB)")

tab1, tab2 = st.tabs(["고객조회", "거래 입력 및 고객 등록"])

# --- 탭 1: 고객 조회 및 정보 가져오기 ---
with tab1:
    st.header("고객 조회")
    name = st.text_input("조회할 고객명", key="search_name")
    
    # 고객 이름 입력 시 조회 시작
    if len(name) > 0:
        # 고객과 주문 정보를 조인하여 조회
        sql_select = f"""
        SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
        FROM Customer c 
        LEFT JOIN Orders o ON c.custid = o.custid 
        LEFT JOIN Book b ON o.bookid = b.bookid
        WHERE c.name = '{name}'
        ORDER BY o.orderdate DESC NULLS LAST;
        """
        
        # DuckDB에서 DataFrame으로 바로 결과 받기
        result_df = query(sql_select, fetch_type='df')
        
        if result_df is not None and not result_df.empty:
            
            # 모든 결과가 NULL이 아닌 행만 필터링하여 주문 내역 표시
            order_history = result_df[result_df['bookname'].notna()]
            
            st.subheader(f"'{name}' 님의 주문 내역")
            if not order_history.empty:
                st.dataframe(order_history[['bookname', 'orderdate', 'saleprice']], use_container_width=True)
            else:
                st.info(f"'{name}' 님의 주문 내역이 없습니다.")
                
            # custid와 name을 다음 탭으로 전달하기 위해 저장
            # DuckDB는 custid를 int64로 반환하므로 .iloc[0]으로 첫 번째 값 사용
            custid = result_df['custid'].iloc[0]
            st.session_state['current_custid'] = custid
            st.session_state['current_name'] = name
            
            st.caption(f"**현재 고객 번호:** {custid}")

        else:
            # 이름이 DB에 없는 경우, 신규 등록 안내
            st.warning(f"고객 '{name}'을(를) 찾을 수 없습니다. 새로운 고객으로 등록하려면 '거래 입력 및 고객 등록' 탭을 이용하세요.")
            st.session_state['current_custid'] = None
            st.session_state['current_name'] = name


# --- 탭 2: 거래 입력 및 고객 등록 ---
with tab2:
    st.header("거래 입력 및 고객 등록")
    
    # 세션 상태에서 고객 정보 가져오기
    current_custid = st.session_state.get('current_custid')
    current_name = st.session_state.get('current_name', "")

    # 과제 요구사항: 신규 고객 등록
    st.subheader("신규 고객 등록 (과제)")
    new_name = st.text_input("등록할 이름 (필수)", key="new_cust_name")
    new_address = st.text_input("주소")
    new_phone = st.text_input("전화번호 (예: 010-1234-5678)")
    
    if st.button("고객 등록"):
        if new_name:
            # 최대 custid 조회 후 +1
            max_id_df = query("SELECT MAX(custid) AS max_id FROM Customer", 'df')
            new_custid = max_id_df['max_id'].iloc[0] + 1 if max_id_df is not None and not max_id_df.empty and max_id_df['max_id'].iloc[0] is not None else 1
            
            # SQL Injection 방지를 위해 파이썬 문자열 포매팅 사용 (실제 서비스에서는 파라미터 바인딩 권장)
            sql_insert_cust = f"""
            INSERT INTO Customer (custid, name, address, phone) 
            VALUES ({new_custid}, '{new_name}', '{new_address}', '{new_phone}');
            """
            
            query(sql_insert_cust, fetch_type='none')
            st.success(f"✅ 고객 '{new_name}' (ID: {new_custid})이(가) 성공적으로 등록되었습니다.")
            st.session_state['current_custid'] = new_custid # 등록 후 바로 사용 가능하도록 세션 업데이트
            st.session_state['current_name'] = new_name
            
            # Streamlit 재실행 (입력 필드 초기화)
            st.rerun() 
        else:
            st.warning("등록할 고객 이름은 필수입니다.")

    st.markdown("---")
    
    # --- 거래 입력 ---
    st.subheader("도서 거래 입력")
    
    if current_custid:
        st.info(f"현재 고객: **{current_name}** (ID: **{current_custid}**)")
        
        # 도서 선택
        select_book = st.selectbox("구매 서적:", books, key="purchase_book_select")

        if select_book and select_book != 'None':
            # 선택된 책 정보 파싱
            bookid_str, bookname = select_book.split(",", 1)
            bookid = int(bookid_str)
            
            # 가격 입력
            price = st.number_input(f"구매 금액 ({bookname})", min_value=1, step=1000, key="purchase_price_input")
            
            # 날짜 설정
            dt = time.strftime('%Y-%m-%d', time.localtime())
            
            if st.button('거래 입력 (과제)', key="insert_order_btn"):
                # 최대 orderid 조회 후 +1
                max_orderid_df = query("SELECT MAX(orderid) AS max_id FROM Orders", 'df')
                new_orderid = max_orderid_df['max_id'].iloc[0] + 1 if max_orderid_df is not None and not max_orderid_df.empty and max_orderid_df['max_id'].iloc[0] is not None else 1
                
                sql_insert_order = f"""
                INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                VALUES ({new_orderid}, {current_custid}, {bookid}, {price}, '{dt}');
                """
                
                # DuckDB에 인서트 및 커밋
                query(sql_insert_order, fetch_type='none')
                
                # 인서트 후 확인 메시지
                st.success(f"🎉 거래가 성공적으로 입력되었습니다! (주문 ID: {new_orderid})")
                
                # 주문 내역을 즉시 확인하려면 '고객 조회' 탭으로 이동하세요.

        else:
            st.warning("구매할 도서를 선택하세요.")
    else:
        st.warning("거래를 입력하려면 '고객 조회' 탭에서 고객을 조회하거나, 상단에서 신규 고객을 등록하세요.")
        st.session_state['current_name'] = new_name # 등록한 고객 이름이 표시되도록 업데이트


# --- 연결 종료 (Streamlit은 스크립트 실행 후 자동으로 종료되므로 필요하지 않지만, 습관적으로 추가) ---
# conn.close()
