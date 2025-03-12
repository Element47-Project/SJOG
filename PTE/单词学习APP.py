import pandas as pd
import random
import tkinter as tk
from tkinter import simpledialog
import requests


def load_vocab(file_path, sheet_name):
    """加载Excel中的单词表，支持选择Sheet。
       假设Excel文件中已经有 'Check' 列，如果没有请自行处理。
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except Exception as e:
        print(f"加载文件失败: {e}")
        return None


def update_excel(df, file_path, sheet_name):
    """更新Excel文件的Sheet内容"""
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            for sheet in pd.ExcelFile(file_path).sheet_names:
                if sheet == sheet_name:
                    df.to_excel(writer, sheet_name=sheet, index=False)
                else:
                    pd.read_excel(file_path, sheet_name=sheet).to_excel(writer, sheet_name=sheet, index=False)
    except Exception as e:
        print(f"保存文件失败: {e}")


def fetch_example_sentence(word):
    """Fetch example sentence for a given word using an API"""
    try:
        response = requests.get(f"https://api.example.com/sentence?word={word}")
        if response.status_code == 200:
            return response.json().get('sentence', 'No example sentence found.')
        else:
            return 'No example sentence found.'
    except Exception as e:
        print(f"Error fetching example sentence: {e}")
        return 'No example sentence found.'


def start_ui(df, file_path, sheet_name, max_words):
    """
    创建带有UI界面的单词记忆程序，5轮完成或单词用尽后自动换下一批。
    max_words: 每批最多抽取多少单词 (如 50, 20, 10).
    """
    root = tk.Tk()
    root.title("PTE 单词记忆")
    root.geometry("800x600")
    root.configure(bg="#f0f0f0")

    vocab_list = []
    rounds_completed = 0
    words_shown = 0
    current_index = 0

    label_progress = tk.Label(root, text="", font=("Arial", 12), bg="#f0f0f0")
    label_progress.pack(pady=10)

    label_word = tk.Label(root, text="", font=("Arial", 16, "bold"), bg="#f0f0f0")
    label_word.pack(pady=10)

    label_pos = tk.Label(root, text="", font=("Arial", 12), fg="green", bg="#f0f0f0")
    label_pos.pack(pady=5)

    label_answer = tk.Label(root, text="", font=("Arial", 12), fg="blue", bg="#f0f0f0")
    label_answer.pack(pady=10)

    button_style = {"width": 15, "height": 2, "bg": "#4CAF50", "fg": "white", "font": ("Arial", 10)}

    def save_and_exit():
        update_excel(df, file_path, sheet_name)
        root.quit()

    def update_display():
        """刷新UI显示，不改变任何计数逻辑。"""
        if vocab_list:
            w, _, _ = vocab_list[current_index]
            label_word.config(text=f"单词: {w}")
            label_pos.config(text="")
            label_answer.config(text="")
            label_progress.config(
                text=f"轮次: {rounds_completed + 1}/5, 单词: {current_index + 1}/{len(vocab_list)}"
            )
        else:
            label_word.config(text="所有单词都已记住！")

    def reload_words():
        nonlocal vocab_list, rounds_completed, words_shown, current_index
        new_vocab = df[df['Check'] == 0][['Word', 'Meaning', 'Part of Speech']].values.tolist()
        if len(new_vocab) == 0:
            label_word.config(text="所有单词都已记住，无需继续学习！")
            return

        if len(new_vocab) > max_words:
            new_vocab = random.sample(new_vocab, max_words)

        vocab_list[:] = new_vocab
        rounds_completed = 0
        words_shown = 0
        current_index = 0
        update_display()

    def next_word():
        """用户点击【下一个】时：words_shown+1，移动索引，判断是否完成一轮或 5 轮。"""
        nonlocal current_index, rounds_completed, words_shown

        if not vocab_list:
            return

        words_shown += 1
        current_index += 1
        if current_index >= len(vocab_list):
            current_index = 0

        if current_index == 0:
            rounds_completed += 1
            words_shown = 0
            if rounds_completed >= 5:
                reload_words()
                return

        update_display()

    def mark_known():
        """标记当前单词为已记住 (Check=1) 并移除"""
        nonlocal current_index, rounds_completed
        if not vocab_list:
            return

        w, _, _ = vocab_list[current_index]
        df.loc[df['Word'] == w, 'Check'] = 1
        vocab_list.pop(current_index)

        if not vocab_list:
            reload_words()
            return

        if current_index >= len(vocab_list):
            current_index = 0
            rounds_completed += 1

        update_display()

    def mark_memorized():
        """删除单词 (从 df 中彻底移除)"""
        nonlocal current_index
        if not vocab_list:
            return

        w, _, _ = vocab_list[current_index]
        df.drop(df[df['Word'] == w].index, inplace=True)
        vocab_list.pop(current_index)

        if not vocab_list:
            reload_words()
            return

        if current_index >= len(vocab_list):
            current_index = 0
        update_display()

    def mark_familiar():
        """将当前单词的Check标记为2，表示熟悉"""
        nonlocal current_index
        if not vocab_list:
            return

        w, _, _ = vocab_list[current_index]
        df.loc[df['Word'] == w, 'Check'] = 2
        vocab_list.pop(current_index)

        if not vocab_list:
            reload_words()
            return

        if current_index >= len(vocab_list):
            current_index = 0
        update_display()

    def show_answer():
        if vocab_list:
            _, meaning, pos = vocab_list[current_index]
            label_pos.config(text=f"词性: {pos}")
            label_answer.config(text=f"释义: {meaning}")

    button_known = tk.Button(root, text="记住了", command=mark_known, **button_style)
    button_known.pack(pady=5)

    button_show = tk.Button(root, text="显示答案", command=show_answer, **button_style)
    button_show.pack(pady=5)

    button_next = tk.Button(root, text="下一个", command=next_word, **button_style)
    button_next.pack(pady=5)

    button_memorized = tk.Button(root, text="牢记 (删除)", command=mark_memorized, **button_style)
    button_memorized.pack(pady=5)

    button_familiar = tk.Button(root, text="熟悉", command=mark_familiar, **button_style)
    button_familiar.pack(pady=5)

    button_exit = tk.Button(root, text="退出", command=save_and_exit, **button_style)
    button_exit.pack(pady=5)

    reload_words()
    root.mainloop()


if __name__ == "__main__":
    file_path = r"C:\Users\Shane\Desktop\PTE\PTE_High_Frequency_Vocabulary.xlsx"
    sheet_name = "Sheet" + simpledialog.askstring("输入", "请输入要使用的Sheet名称:")
    max_words = 0
    if sheet_name == "Sheet1":
        max_words = 50
    elif sheet_name == "Sheet2":
        max_words = 20
    elif sheet_name == "Sheet3":
        max_words = 10
    elif sheet_name == "Sheet4":
        max_words = 5

    df = load_vocab(file_path, sheet_name)
    if df is not None:
        start_ui(df, file_path, sheet_name, max_words)
