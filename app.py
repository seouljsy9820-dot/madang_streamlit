import streamlit as st
import duckdb
import pandas as pd
import time
import os

# --- 2. DB 연결 설정 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "madang.db")   # ← DB 절대경로

# DuckDB 연결
conn = duckdb.connect(database=DB_FILE)

# --- 3. 쿼리 실행 함수 ---
def query(sql, fetch_type='df'):
    """DuckDB에 SQL 쿼리를 실행하고 결과를 반환합니다."""
    try:
        # SELECT
        if sql.strip().upper().startswith('SELECT'):
            if fetch_type == 'df':
                return conn.execute(sql).fetchdf()
            else:
                return conn.execute(sql).fetchall()

        # INSERT, UPDATE
        else:
            conn.execute(sql)
            conn.commit()
            return None

    except Exception as e:
        st.error(f"데이터베이스 쿼리 실행 중 오류 발생: {e}")
        return None


# --- 초기 도서 목록 로딩 ---
books = [None]

result_df = query("SELECT bookid, bookname FROM Book")

if result_df is not None:
    for index, row in result_df.iterrows():
        books.append(f"{row['bookid']},{row['bookname']}")
else:
    st.error("도서 목록 불러오기 실패. madang.db 위치를 확인하세요.")


# ==========================
#    Streamlit UI 시작
# ==========================

st.title("📚 마당 도서 관리 시스템 (DuckDB)")

tab1, tab2 = st.tabs(["고객조회", "거래 입력 및 고객 등록"])


# =====================================
#          탭 1: 고객 조회
# =====================================
with tab1:
    st.header("고객 조회")
    name = st.text_input("조회할 고객명", key="search_name")

    if len(name) > 0:

        sql_select = f"""
        SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
        FROM Customer c 
        LEFT JOIN Orders o ON c.custid = o.custid 
        LEFT JOIN Book b ON o.bookid = b.bookid
        WHERE c.name = '{name}'
        ORDER BY o.orderdate DESC NULLS LAST;
        """

        result_df = query(sql_select, fetch_type='df')

        if result_df is not None and not result_df.empty:

            # --- 날짜를 'YYYY-MM-DD 00:00:00' 형식으로 통일 ---
            if 'orderdate' in result_df.columns:
                result_df['orderdate'] = result_df['orderdate'].astype(str)
                result_df['orderdate'] = result_df['orderdate'].apply(
                    lambda x: x if ' ' in x else x + ' 00:00:00'
                )

            # 주문 내역 필터
            order_history = result_df[result_df['bookname'].notna()]

            st.subheader(f"'{name}' 님의 주문 내역")
            if not order_history.empty:
                st.dataframe(
                    order_history[['bookname', 'orderdate', 'saleprice']],
                    use_container_width=True
                )
            else:
                st.info(f"'{name}' 님의 주문 내역이 없습니다.")

            # 고객 번호 유지
            custid = result_df['custid'].iloc[0]
            st.session_state['current_custid'] = custid
            st.session_state['current_name'] = name

            st.caption(f"**현재 고객 번호:** {custid}")

        else:
            st.warning(f"고객 '{name}'이(가) 존재하지 않습니다.")
            st.session_state['current_custid'] = None
            st.session_state['current_name'] = name



# =====================================
#      탭 2: 고객 등록 + 거래 입력
# =====================================
with tab2:
    st.header("거래 입력 및 고객 등록")

    current_custid = st.session_state.get('current_custid')
    current_name = st.session_state.get('current_name', "")

    # 신규 고객 등록
    st.subheader("신규 고객 등록 (과제)")
    new_name = st.text_input("등록할 이름 (필수)", key="new_cust_name")
    new_address = st.text_input("주소")
    new_phone = st.text_input("전화번호 (예: 010-1234-5678)")

    if st.button("고객 등록"):
        if new_name:
            max_id_df = query("SELECT MAX(custid) AS max_id FROM Customer", 'df')
            new_custid = (
                max_id_df['max_id'].iloc[0] + 1
                if max_id_df is not None and not max_id_df.empty and max_id_df['max_id'].iloc[0] is not None
                else 1
            )

            sql_insert_cust = f"""
            INSERT INTO Customer (custid, name, address, phone) 
            VALUES ({new_custid}, '{new_name}', '{new_address}', '{new_phone}');
            """

            query(sql_insert_cust, fetch_type='none')
            st.success(f"✅ 고객 '{new_name}' (ID: {new_custid}) 신규 등록 완료!")

            st.session_state['current_custid'] = new_custid
            st.session_state['current_name'] = new_name

            st.rerun()
        else:
            st.warning("고객 이름은 반드시 입력해야 합니다.")

    st.markdown("---")

    # 거래 입력
    st.subheader("도서 거래 입력")

    if current_custid:
        st.info(f"현재 고객: **{current_name}** (ID: **{current_custid}**)")

        select_book = st.selectbox("구매 서적:", books, key="purchase_book_select")

        if select_book and select_book != 'None':
            bookid_str, bookname = select_book.split(",", 1)
            bookid = int(bookid_str)

            price = st.number_input(f"구매 금액 ({bookname})", min_value=1, step=1000)

            dt = time.strftime('%Y-%m-%d', time.localtime())

            if st.button("거래 입력 (과제)"):
                max_orderid_df = query("SELECT MAX(orderid) AS max_id FROM Orders", 'df')
                new_orderid = (
                    max_orderid_df['max_id'].iloc[0] + 1
                    if max_orderid_df is not None and not max_orderid_df.empty and max_orderid_df['max_id'].iloc[0] is not None
                    else 1
                )

                sql_insert_order = f"""
                INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                VALUES ({new_orderid}, {current_custid}, {bookid}, {price}, '{dt}');
                """

                query(sql_insert_order, fetch_type='none')

                st.success(f"🎉 거래 입력 완료! (주문 ID: {new_orderid})")

        else:
            st.warning("도서를 선택하세요.")
    else:
        st.warning("거래 입력하려면 먼저 고객을 조회하거나 신규 고객을 등록하세요.")
