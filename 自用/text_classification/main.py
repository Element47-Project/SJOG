from datasets import load_dataset

# 加载IMDB数据集
dataset = load_dataset('imdb')
print(dataset)  # 检查数据集结构

dataset.save_to_disk('data')