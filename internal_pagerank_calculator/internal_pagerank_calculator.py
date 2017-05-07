import csv
import codecs
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, update
import numpy as np

# 調べたいドメイン(例: http://re1ven.com) (別の例: https://google.com)
base_url = "https://example.com" # 仮アドレスを書いておく
# screaming flogで取ってきたoutlinkのcsvファイルのパス)
csv_file_name = "outlink.csv"

engine = create_engine('sqlite:///data.sqlite3')
metadata = MetaData(engine)

# table: page_link_raw (csvデータで必要なものを入れておく)
page_link_raw = Table('page_link_data', metadata,
                      Column('id', Integer(), primary_key=True),
                      Column('from_page_url', String()),
                      Column('from_page_id', Integer()),
                      Column('to_page_url', String()),
                      Column('to_page_id', Integer())
                      )

# table: page_data (pageにinteger primary keyなidを割り振って、idとpageのurlのデータを保存)
page_data = Table('page_data', metadata,
                  Column('id', Integer(), primary_key=True),
                  # Column('page_title', String()), # titleはちょっと取りにくいのでパス
                  Column('page_url', String(), unique=True)
                  )

# table: page_link_count (ページの有向グラフ行列のためのデータテーブル)
page_link_count = Table('page_link_count', metadata,
                        Column('id', Integer(), primary_key=True),
                        Column('from_page_id', Integer()),
                        Column('to_page_id', Integer()),
                        Column('count', Integer(), default=0)
                        )

metadata.create_all(engine)
connection = engine.connect()
transaction = connection.begin()

# csv処理
with codecs.open(csv_file_name, 'r', 'utf-8') as file:
    csv_reader = csv.reader(file, delimiter=',', quotechar='"')
    for item in csv_reader:
        # リンクかつ内部ページから内部ページでへの遷移であることを保証する
        if item[0] == 'HREF' and item[1].find(base_url) == 0 and item[2].find(base_url) == 0:
            ins_pagedata = page_data.insert().values(
                page_url=item[1]
            )
            ins_page_to_data = page_data.insert().values(
                page_url=item[2]
            )
            ins_page_link_data = page_link_raw.insert().values(
                from_page_url=item[1],
                to_page_url=item[2]
            )
            try:
                result = connection.execute(ins_pagedata)
            except:
                pass
            try:
                result = connection.execute(ins_pagedata)
            except:
                pass
            try:
                result = connection.execute(ins_page_link_data)
            except:
                pass

s = select([page_data.c.id, page_data.c.page_url])

all_rows = connection.execute(s)
row_list = list()
url_list = list()

for row in all_rows:
    row_list.append(row)
    url_list.append(row.page_url)

# table: link_count (有向グラフの行列のためのdatabaseの情報の初期化)
for row in row_list:
    for col in row_list:
        ins = page_link_count.insert().values(
            from_page_id=row.id,
            to_page_id=col.id
        )
        try:
            result = connection.execute(ins)
        except:
            pass

# table: page_link_rawのリンクデータにidをふる
for row in row_list:
    u = update(page_link_raw).where(page_link_raw.c.from_page_url == row.page_url)
    u = u.values(from_page_id=row.id)
    try:
        result = connection.execute(u)
    except:
        pass
    u = update(page_link_raw).where(page_link_raw.c.to_page_url == row.page_url)
    u = u.values(to_page_id=row.id)
    try:
        result = connection.execute(u)
    except:
        pass

# from_id, to_idへのリンク遷移を記録
s = select([page_link_raw.c.from_page_id, page_link_raw.c.to_page_id])
id_pair_list = connection.execute(s)
for id_pair in id_pair_list:
    from_page_id = id_pair.from_page_id
    to_page_id = id_pair.to_page_id
    u = update(page_link_count).where(page_link_count.c.from_page_id == from_page_id).where(
        page_link_count.c.to_page_id == to_page_id)
    u = u.values(count=page_link_count.c.count + 1)
    try:
        result = connection.execute(u)
    except:
        pass

# 行列計算開始
rank = len(row_list)
matrix_list = list()
for row in row_list:
    s = select([page_link_count.c.from_page_id, page_link_count.c.count])
    s = s.where(page_link_count.c.from_page_id == row.id)
    row_items = connection.execute(s)
    matrix_row_list = list()
    for row_item in row_items:
        matrix_row_list.append(row_item.count)
    matrix_list.append(matrix_row_list)

array_raw = np.array(matrix_list)
array = list()
for row in array_raw.T:
    array.append(row / np.sum(row))
array = np.array(array).T

modified = np.ones(array.shape, dtype=np.float) / rank
# 調整パラメータ 参照:(http://www.geocities.jp/existenzueda/pagerank_actual.htm)
alpha = 0.15

google_matrix = alpha * modified + (1 - alpha) * array
d = np.ones((rank, 1), dtype=np.float) / rank

current = np.dot(google_matrix, d)
current_matrix = google_matrix
for i in range(2000): #とりあえず2000回ほど回しておく(多分だいたいある程度収束するっぽい)
    current_matrix = np.dot(current_matrix, google_matrix)
    current = np.dot(current_matrix, d)

# ページランクは各ページへの遷移確率なので、総和は1になる。そのことを確認
print(np.sum(current))
print(current)

transaction.commit()
