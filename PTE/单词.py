import pandas as pd
import requests

# 读取上传的 Excel 文件
file_path = r"C:\Users\Shane\Desktop\PTE\New Word.xlsx"
df = pd.read_excel(file_path)

# 确保存在 "Word" 列，如果缺少 "Part of Speech" 或 "Meaning" 列，则添加空列
if "Part of Speech" not in df.columns:
    df["Part of Speech"] = None
if "Meaning" not in df.columns:
    df["Meaning"] = None

# 筛选出需要补充词性或释义的单词
missing_info_words = df[(df["Meaning"].isna()) | (df["Part of Speech"].isna())]

def get_word_info_online(word):
    """
    使用 DictionaryAPI.dev 免费 API 获取单词的词性和释义。
    API 地址: https://api.dictionaryapi.dev/api/v2/entries/en/<word>
    """
    url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            return "Unknown", "No definition found"
        data = response.json()
        meanings = data[0].get("meanings", [])
        if meanings:
            # 取第一个含义记录
            first_meaning = meanings[0]
            pos = first_meaning.get("partOfSpeech", "Unknown")
            defs = first_meaning.get("definitions", [])
            if defs:
                definition = defs[0].get("definition", "No definition found")
            else:
                definition = "No definition found"
        else:
            pos, definition = "Unknown", "No definition found"
        return pos, definition
    except Exception as e:
        # 出现异常时返回默认值
        return "Unknown", "No definition found"

# 对缺失信息的单词逐一补充词性和释义
for index, row in missing_info_words.iterrows():
    word = row["Word"]
    pos, meaning = get_word_info_online(word)
    df.at[index, "Part of Speech"] = pos
    df.at[index, "Meaning"] = meaning

# 保存更新后的 Excel 文件
updated_file_path = r"C:\Users\Shane\Desktop\PTE\New Word.xlsx"
df.to_excel(updated_file_path, index=False)
print("更新后的文件保存在:", updated_file_path)
