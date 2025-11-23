import streamlit as st
import duckdb  # 1. duckdb 사용
import pandas as pd
import time

# --- 2. DB 연결 설정 ---
# Streamlit 앱이 시작될 때 madang.db 파일에 연결
DB_FILE = 'madang.db'
conn = duckdb.connect(database=DB_FILE)

# --- 3. 쿼리 함수 ---
def query(sql, fetch_type='df'):
    """DuckDB에 SQL 쿼리를 실행하고 결과를 반환"""
    try:
        # SELECT 쿼리
        if sql.strip().upper().startswith('SELECT'):
            if fetch_type == 'df':
                return conn.execute(sql).fetchdf()
            else:
                return conn.execute(sql).fetchall()
        else:
            # INSERT, UPDATE 등
            conn.execute(sql)
            conn.commit()
            return None
    except Exception as e:
        st.error(f"쿼리 오류 발생: {e}")
        return None


# --- Book 목록 불러오기 ---
books = [None]
result_df = query("SELECT bookid, bookname FROM Book")

if result_df is not None:
    for index, row in result_df.iterrows():
        books.append(f"{row['bookid']},{row['bookname']}")
else:
    st.error("도서 목록을 불러오지 못했습니다.")


# --- Streamlit UI ---
st.title("📚 마당 도서 관리 시스템 (DuckDB)")

tab1, tab2 = st.tabs(["고객조회", "거래 입력 및 고객 등록"])

# =========================
# 탭 1: 고객 조회
# =========================
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
        ORDER ORDER BY o.orderdate DESC NULLS LAST;
        """

        result_df = query(sql_select, fetch_type='df')

        if result_df is not None and not result_df.empty:
            order_history = result_df[result_df['bookname'].notna()]

            st.subheader(f"'{name}' 님의 주문 내역")
            if not order_history.empty:
                st.dataframe(order_history[['bookname', 'orderdate', 'saleprice']], use_container_width=True)
            else:
                st.info(f"'{name}' 님의 주문 내역이 없습니다.")

            custid = result_df['custid'].iloc[0]
            st.session_state['current_custid'] = custid
            st.session_state['current_name'] = name

            st.caption(f"현재 고객 번호: {custid}")

        else:
            st.warning(f"고객 '{name}'이(가) 없습니다. 신규 등록하려면 두 번째 탭을 이용하세요.")
            st.session_state['current_custid'] = None
            st.session_state['current_name'] = name



# =========================
# 탭 2: 신규 고객 등록 + 거래 입력
# =========================
with tab2:
    st.header("거래 입력 및 고객 등록")

    current_custid = st.session_state.get('current_custid')
    current_name = st.session_state.get('current_name', "")

    st.subheader("신규 고객 등록 (과제)")
    new_name = st.text_input("등록할 이름 (필수)", key="new_cust_name")
    new_address = st.text_input("주소")
    new_phone = st.text_input("전화번호 (예: 010-1234-5678)")

    if st.button("고객 등록"):
        if new_name:
            max_id_df = query("SELECT MAX(custid) AS max_id FROM Customer", 'df')
            new_custid = max_id_df['max_id'].iloc[0] + 1 if max_id_df['max_id'].iloc[0] is not None else 1

            sql_insert_cust = f"""
            INSERT INTO Customer (custid, name, address, phone) 
            VALUES ({new_custid}, '{new_name}', '{new_address}', '{new_phone}');
            """

            query(sql_insert_cust, fetch_type='none')
            st.success(f"고객 '{new_name}' (ID: {new_custid}) 등록 완료!")

            st.session_state['current_custid'] = new_custid
            st.session_state['current_name'] = new_name

            st.rerun()
        else:
            st.warning("고객 이름은 필수입니다.")

    st.markdown("---")

    st.subheader("도서 거래 입력")

    if current_custid:
        st.info(f"현재 고객: {current_name} (ID: {current_custid})")

        select_book = st.selectbox("구매 서적:", books)

        if select_book and select_book != 'None':
            bookid_str, bookname = select_book.split(",", 1)
            bookid = int(bookid_str)

            price = st.number_input(f"구매 금액 ({bookname})", min_value=1, step=1000)

            dt = time.strftime('%Y-%m-%d')

            if st.button("거래 입력 (과제)"):
                max_orderid_df = query("SELECT MAX(orderid) AS max_id FROM Orders", 'df')
                new_orderid = max_orderid_df['max_id'].iloc[0] + 1 if max_orderid_df['max_id'].iloc[0] is not None else 1

                sql_insert_order = f"""
                INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                VALUES ({new_orderid}, {current_custid}, {bookid}, {price}, '{dt}');
                """

                query(sql_insert_order, fetch_type='none')
                st.success(f"거래 입력 완료! (주문 ID: {new_orderid})")

    else:
        st.warning("고객을 조회하거나 신규 고객을 등록하세요.")
