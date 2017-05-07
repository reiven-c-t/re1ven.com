import numpy as np

# サンプルのページ間の有向グラフの行列の定義
array_raw = np.array(
    [
        [0,0,1],
        [1,0,0],
        [1,1,0]
    ])
array_matrix = list()

for row in array_raw.T:
    array_matrix_append = row / np.sum(row)
    array_matrix.append(array_matrix_append)

array = np.array(array_matrix).T
# 有向グラフの配列の遷移確率処理を確認
print(array)

# 初期ベクトルの定義
d = np.ones((3, 1),dtype=np.float) / 3

# page rank計算(パワー法)
current = np.dot(array, d)
current_matrix = array
for i in range(50):
    current_matrix = np.dot(current_matrix, array)
    current = np.dot(current_matrix, d)

# page_rankの表示
print(current)
